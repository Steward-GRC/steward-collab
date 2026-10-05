// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	grpcactor "github.com/Bugs5382/go-grpc-actor"
	log "github.com/Bugs5382/go-log"
	collabv1 "github.com/Steward-GRC/steward-collab/gen/go/steward/collab/v1"
	"github.com/Steward-GRC/steward-collab/internal/audit"
	"github.com/Steward-GRC/steward-collab/internal/grpcsvc"
	"github.com/Steward-GRC/steward-collab/internal/snapshot"
	"github.com/Steward-GRC/steward-collab/internal/store"
	"github.com/Steward-GRC/steward-collab/internal/ws"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// These tests cross both wires end to end: a real gRPC connection into
// CollabRoomService, carrying go-grpc-actor's client and server interceptors,
// and real websocket clients on the real ws.Handler over the same hub the RPC
// is wired to. Driving the hub directly could never show that the RPC reaches
// the editors.

// trustAll believes every forwarded actor: the bufconn hop has no TLS peer to
// check.
func trustAll(context.Context, string) bool { return true }

// roomMemStore is an in-memory store.Store: the room only needs somewhere to
// load from and save to.
type roomMemStore struct {
	mu   sync.Mutex
	docs map[string]store.Document
}

func newRoomMemStore() *roomMemStore { return &roomMemStore{docs: map[string]store.Document{}} }

func (m *roomMemStore) Load(_ context.Context, draftID string) (*store.Document, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.docs[draftID]
	if !ok {
		return nil, nil
	}
	cp := d
	return &cp, nil
}

func (m *roomMemStore) Save(_ context.Context, doc store.Document) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.docs[doc.DraftID] = doc
	return nil
}

// collabTestbed is a running collab process in miniature: one hub, the real
// WebSocket handler in front of it, and the real CollabRoomService gRPC handler
// wired to the SAME hub — which is the thing that has to hold true in
// production for the notification to reach anybody.
type collabTestbed struct {
	hub    *ws.Hub
	wsURL  string
	client collabv1.CollabRoomServiceClient
	audits *fakeAuditPublisher
}

// newCollabTestbed stands the testbed up. The actor travels in wire metadata,
// not in-process.
func newCollabTestbed(t *testing.T) *collabTestbed {
	t.Helper()
	return newCollabTestbedWithPublisher(t, nil)
}

// newCollabTestbedWithPublisher is newCollabTestbed with the hub's snapshot
// publisher supplied, for tests that need checkpoints to reach a core.
func newCollabTestbedWithPublisher(t *testing.T, pub *snapshot.Publisher) *collabTestbed {
	t.Helper()

	emitter, audits := newTestEmitter()
	hub := ws.NewHub(newRoomMemStore(), pub, nil, log.Nop())
	go hub.Run(t.Context())

	httpSrv := httptest.NewServer(http.HandlerFunc(
		ws.NewHandler(hub, testSecret, log.Nop()).ServeHTTP))
	t.Cleanup(httpSrv.Close)

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(grpcactor.UnaryServerInterceptor(grpcactor.WithTrust(trustAll))))
	grpcsvc.RegisterRoomService(srv, grpcsvc.NewRoomService(hub, emitter, log.Nop()))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(grpcactor.UnaryClientInterceptor()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return &collabTestbed{
		hub:    hub,
		wsURL:  "ws" + strings.TrimPrefix(httpSrv.URL, "http") + "/ws/",
		client: collabv1.NewCollabRoomServiceClient(conn),
		audits: audits,
	}
}

// join opens a real WebSocket session on the testbed for draftID with a token
// the handler actually verifies, and returns the connection. It waits until the
// hub reports the client as registered so a following RPC cannot race the join.
func (tb *collabTestbed) join(t *testing.T, draftID, policyID, userID string) *websocket.Conn {
	t.Helper()

	claims := ws.CollabClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Audience:  jwt.ClaimStrings{ws.TokenAudience},
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			NotBefore: jwt.NewNumericDate(time.Now().Add(-time.Second)),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
		},
		UserID:            userID,
		PolicyID:          policyID,
		DraftID:           draftID,
		TemplateVersionID: "tv-1",
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(testSecret))
	if err != nil {
		t.Fatalf("sign collab token: %v", err)
	}

	conn, resp, err := websocket.DefaultDialer.Dial(tb.wsURL+draftID+"?token="+signed, nil)
	if err != nil {
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		t.Fatalf("dial collab websocket: %v (http %d)", err, code)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// waitForEditors blocks until the hub's room for draftID reports at least n
// connected editors. Registration happens on the room goroutine, so a test
// that fires the RPC immediately after Dial can otherwise find an empty room.
func (tb *collabTestbed) waitForEditors(t *testing.T, draftID string, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if info, ok := tb.hub.LookupRoom(draftID); ok && info.Editors >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("room %q never reached %d editors", draftID, n)
}

// readControl reads frames off conn until a MsgControl arrives, skipping the
// sync/awareness traffic a joiner always receives. It fails the test on
// timeout, so "the editor was never told" is a failure and not a hang.
func readControl(t *testing.T, conn *websocket.Conn) ws.ControlMessage {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read websocket frame while waiting for a control frame: %v", err)
		}
		msgType, payload, err := ws.DecodeMessage(raw)
		if err != nil {
			t.Fatalf("decode websocket frame: %v", err)
		}
		if msgType != ws.MsgControl {
			continue
		}
		ctrl, err := ws.DecodeControlMessage(payload)
		if err != nil {
			t.Fatalf("decode control frame: %v", err)
		}
		return ctrl
	}
}

// TestNotifyDraftPublished_FreezesEveryLiveEditorOverGRPC: a real gRPC
// NotifyDraftPublished call must reach the live room and put a draft.published
// frame on EVERY connected editor's socket.
func TestNotifyDraftPublished_FreezesEveryLiveEditorOverGRPC(t *testing.T) {
	tb := newCollabTestbed(t)

	bob := tb.join(t, "draft-1", "policy-1", "u-bob")
	carol := tb.join(t, "draft-1", "policy-1", "u-carol")
	tb.waitForEditors(t, "draft-1", 2)

	resp, err := tb.client.NotifyDraftPublished(
		actorCtx(t, publisher()),
		&collabv1.NotifyDraftPublishedRequest{
			DraftId: "draft-1", PolicyId: "policy-1", VersionNumber: 7,
		})
	if err != nil {
		t.Fatalf("NotifyDraftPublished across the gateway hop: %v", err)
	}
	if !resp.GetRoomNotified() {
		t.Fatal("room_notified = false with two live editors attached")
	}

	for name, conn := range map[string]*websocket.Conn{"bob": bob, "carol": carol} {
		ctrl := readControl(t, conn)
		if ctrl.Type != ws.ControlDraftPublished {
			t.Errorf("%s: control type = %q, want %q", name, ctrl.Type, ws.ControlDraftPublished)
		}
		if ctrl.DraftID != "draft-1" {
			t.Errorf("%s: control draft_id = %q, want draft-1", name, ctrl.DraftID)
		}
		if ctrl.VersionNumber != 7 {
			t.Errorf("%s: control version_number = %d, want 7", name, ctrl.VersionNumber)
		}
	}
}

// TestNotifyDraftPublished_NoRoomIsSilentSuccess: publishing a draft nobody is
// editing is the NORMAL path. It must not be an error, and it must not conjure
// a room — an invented room would leak memory and could later serve stale
// state to a joiner.
func TestNotifyDraftPublished_NoRoomIsSilentSuccess(t *testing.T) {
	tb := newCollabTestbed(t)

	resp, err := tb.client.NotifyDraftPublished(
		actorCtx(t, publisher()),
		&collabv1.NotifyDraftPublishedRequest{
			DraftId: "draft-nobody-is-editing", PolicyId: "policy-1", VersionNumber: 1,
		})
	if err != nil {
		t.Fatalf("publishing an unedited draft returned an error: %v", err)
	}
	if resp.GetRoomNotified() {
		t.Error("room_notified = true when no room existed")
	}
	if got := tb.hub.RoomCount(); got != 0 {
		t.Errorf("hub room count = %d, want 0: the notification conjured a room", got)
	}
	if _, ok := tb.hub.LookupRoom("draft-nobody-is-editing"); ok {
		t.Error("LookupRoom reports a room for a draft nobody is editing")
	}
	if got := tb.audits.snapshot(); len(got) != 0 {
		t.Errorf("audit events = %d, want 0: a no-op must not be audited as a freeze", len(got))
	}
}

// TestNotifyDraftPublished_RefusesPolicyMismatch: policy_id is checked against
// the room's own, so a caller that has the wrong draft/policy pairing cannot
// freeze an unrelated draft's session. The room must be left editable.
func TestNotifyDraftPublished_RefusesPolicyMismatch(t *testing.T) {
	tb := newCollabTestbed(t)

	conn := tb.join(t, "draft-1", "policy-1", "u-bob")
	tb.waitForEditors(t, "draft-1", 1)

	_, err := tb.client.NotifyDraftPublished(
		actorCtx(t, publisher()),
		&collabv1.NotifyDraftPublishedRequest{
			DraftId: "draft-1", PolicyId: "policy-somewhere-else", VersionNumber: 3,
		})
	if err == nil {
		t.Fatal("expected an error for a policy_id that does not match the room")
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("status code = %v, want FailedPrecondition (err: %v)", got, err)
	}

	// The editor must NOT have been frozen. Give the room a moment to have
	// misbehaved, then assert nothing control-shaped arrived.
	if err := conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	for {
		_, raw, readErr := conn.ReadMessage()
		if readErr != nil {
			break // read deadline: nothing more arrived, which is what we want
		}
		msgType, _, decErr := ws.DecodeMessage(raw)
		if decErr == nil && msgType == ws.MsgControl {
			t.Fatal("a mismatched policy_id still froze the room")
		}
	}
}

// TestNotifyDraftPublished_RejectsUnauthenticated: the RPC is AuthN-gated. A
// call with no forwarded actor must be refused rather than allowed to freeze
// rooms anonymously.
func TestNotifyDraftPublished_RejectsUnauthenticated(t *testing.T) {
	tb := newCollabTestbed(t)

	_, err := tb.client.NotifyDraftPublished(context.Background(),
		&collabv1.NotifyDraftPublishedRequest{
			DraftId: "draft-1", PolicyId: "policy-1", VersionNumber: 1,
		})
	if err == nil {
		t.Fatal("expected Unauthenticated without an actor, got success")
	}
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("status code = %v, want Unauthenticated (err: %v)", got, err)
	}
}

// TestNotifyDraftPublished_RejectsUnusableRequests: each field the handler
// needs is required, and version_number must be positive — core uses version_no
// 0 as the draft sentinel, so a zero would announce a publish that never
// happened.
func TestNotifyDraftPublished_RejectsUnusableRequests(t *testing.T) {
	tb := newCollabTestbed(t)
	ctx := actorCtx(t, publisher())

	cases := []struct {
		name string
		req  *collabv1.NotifyDraftPublishedRequest
	}{
		{"missing draft_id", &collabv1.NotifyDraftPublishedRequest{PolicyId: "policy-1", VersionNumber: 1}},
		{"missing policy_id", &collabv1.NotifyDraftPublishedRequest{DraftId: "draft-1", VersionNumber: 1}},
		{"zero version_number", &collabv1.NotifyDraftPublishedRequest{DraftId: "draft-1", PolicyId: "policy-1"}},
		{"negative version_number", &collabv1.NotifyDraftPublishedRequest{DraftId: "draft-1", PolicyId: "policy-1", VersionNumber: -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tb.client.NotifyDraftPublished(ctx, tc.req); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("status code = %v, want InvalidArgument (err: %v)", status.Code(err), err)
			}
		})
	}
}

// TestNotifyDraftPublished_AuditsTheFreeze: freezing a room is a mutating
// action, so it leaves exactly one audit row — attributed to the publishing
// caller, scoped to the policy, and recording how many editors were cut off.
func TestNotifyDraftPublished_AuditsTheFreeze(t *testing.T) {
	tb := newCollabTestbed(t)

	tb.join(t, "draft-1", "policy-1", "u-bob")
	tb.waitForEditors(t, "draft-1", 1)

	if _, err := tb.client.NotifyDraftPublished(
		actorCtx(t, publisher()),
		&collabv1.NotifyDraftPublishedRequest{
			DraftId: "draft-1", PolicyId: "policy-1", VersionNumber: 4,
		}); err != nil {
		t.Fatalf("NotifyDraftPublished: %v", err)
	}

	msgs := tb.audits.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("audit events = %d, want 1: %+v", len(msgs), msgs)
	}
	ev := msgs[0].Event
	if ev.Action != "collab.room.frozen" {
		t.Errorf("audit action = %q, want collab.room.frozen", ev.Action)
	}
	if ev.Tier != audit.TierAudit {
		t.Errorf("audit tier = %v, want %v", ev.Tier, audit.TierAudit)
	}
	if ev.ActorUserID != "u-bob" {
		t.Errorf("audit actor = %q, want u-bob", ev.ActorUserID)
	}
	if ev.Subject != "policy:policy-1" {
		t.Errorf("audit subject = %q, want policy:policy-1", ev.Subject)
	}
	for k, want := range map[string]string{
		"policy_id":      "policy-1",
		"draft_id":       "draft-1",
		"version_number": "4",
		"editors":        "1",
	} {
		if got := ev.Attributes[k]; got != want {
			t.Errorf("audit attribute %s = %q, want %q", k, got, want)
		}
	}
}
