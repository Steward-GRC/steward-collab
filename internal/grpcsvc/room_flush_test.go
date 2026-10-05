// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc_test

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	log "github.com/Bugs5382/go-log"
	collabv1 "github.com/Steward-GRC/steward-collab/gen/go/steward/collab/v1"
	corev1 "github.com/Steward-GRC/steward-collab/gen/go/thirdparty/core/v1"
	"github.com/Steward-GRC/steward-collab/internal/snapshot"
	"github.com/Steward-GRC/steward-collab/internal/ws"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeCore serves core's PolicyService.UpdateDraftContent over a real gRPC
// connection, so the flush crosses the same wire it does in production. A
// non-nil hold keeps each call open until the channel is closed.
type fakeCore struct {
	corev1.UnimplementedPolicyServiceServer

	mu    sync.Mutex
	calls []*corev1.UpdateDraftContentRequest
	err   error
	hold  chan struct{}
}

func (f *fakeCore) UpdateDraftContent(ctx context.Context, req *corev1.UpdateDraftContentRequest) (*corev1.UpdateDraftContentResponse, error) {
	if f.hold != nil {
		select {
		case <-f.hold:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, req)
	if f.err != nil {
		return nil, f.err
	}
	return &corev1.UpdateDraftContentResponse{DraftId: req.GetDraftId(), Accepted: true}, nil
}

func (f *fakeCore) recorded() []*corev1.UpdateDraftContentRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*corev1.UpdateDraftContentRequest(nil), f.calls...)
}

// newFlushTestbed is the collab testbed with its snapshot publisher dialled
// to core over gRPC.
func newFlushTestbed(t *testing.T, core *fakeCore) *collabTestbed {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	corev1.RegisterPolicyServiceServer(srv, core)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///core",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatalf("dial core: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	emitter, _ := newTestEmitter()
	return newCollabTestbedWithPublisher(t, snapshot.NewPublisher(conn, emitter, log.Nop()))
}

func lexical(text string) string {
	return `{"root":{"type":"root","children":[{"type":"paragraph","text":"` + text + `"}]}}`
}

// sendCheckpoint sends a MsgSnapshot the way a browser does and returns once
// the room has recorded it. The server answers a SyncStep1 in read order, so
// its SyncStep2 reply proves the snapshot frame before it was handled.
func sendCheckpoint(t *testing.T, conn *websocket.Conn, contentJSON string) {
	t.Helper()
	if err := conn.WriteMessage(websocket.BinaryMessage, ws.EncodeSnapshot(contentJSON, []byte{0x01})); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, ws.EncodeSync1(ws.EmptyStateVector())); err != nil {
		t.Fatalf("write sync step 1: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("waiting for the room to take the snapshot: %v", err)
		}
		msgType, payload, err := ws.DecodeMessage(raw)
		if err != nil || msgType != ws.MsgSync {
			continue
		}
		if syncType, _, err := ws.DecodeSyncMessage(payload); err == nil && syncType == ws.SyncStep2 {
			return
		}
	}
}

func flushReq() *collabv1.FlushDraftRequest {
	return &collabv1.FlushDraftRequest{DraftId: "draft-1", PolicyId: "policy-1"}
}

// The debounce is left at its production 5s, so core seeing the newest
// checkpoint by the time the RPC returns can only be the flush's doing.
func TestFlushDraft_PersistsTheNewestCheckpointOverGRPC(t *testing.T) {
	core := &fakeCore{}
	tb := newFlushTestbed(t, core)

	conn := tb.join(t, "draft-1", "policy-1", "u-bob")
	tb.waitForEditors(t, "draft-1", 1)
	sendCheckpoint(t, conn, lexical("first"))
	sendCheckpoint(t, conn, lexical("latest"))

	resp, err := tb.client.FlushDraft(actorCtx(t, publisher()), flushReq())
	if err != nil {
		t.Fatalf("FlushDraft: %v", err)
	}
	if !resp.GetRoomFound() || !resp.GetContentFlushed() {
		t.Fatalf("response = %+v, want room_found and content_flushed", resp)
	}
	got := core.recorded()
	if len(got) != 1 {
		t.Fatalf("core UpdateDraftContent calls = %d, want 1", len(got))
	}
	if got[0].GetContentJson() != lexical("latest") {
		t.Errorf("core got %q, want the newest checkpoint", got[0].GetContentJson())
	}
	if got[0].GetPolicyId() != "policy-1" || got[0].GetDraftId() != "draft-1" {
		t.Errorf("core got policy %q draft %q", got[0].GetPolicyId(), got[0].GetDraftId())
	}
}

func TestFlushDraft_ReturnsOnlyAfterCoreAnswers(t *testing.T) {
	core := &fakeCore{hold: make(chan struct{})}
	tb := newFlushTestbed(t, core)

	conn := tb.join(t, "draft-1", "policy-1", "u-bob")
	tb.waitForEditors(t, "draft-1", 1)
	sendCheckpoint(t, conn, lexical("latest"))

	done := make(chan error, 1)
	go func() {
		_, err := tb.client.FlushDraft(actorCtx(t, publisher()), flushReq())
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("FlushDraft returned (err=%v) before core answered", err)
	case <-time.After(300 * time.Millisecond):
	}

	close(core.hold)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("FlushDraft: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("FlushDraft never returned after core answered")
	}
	if got := core.recorded(); len(got) != 1 {
		t.Fatalf("core UpdateDraftContent calls = %d, want 1", len(got))
	}
}

func TestFlushDraft_NoRoomIsSilentSuccess(t *testing.T) {
	core := &fakeCore{}
	tb := newFlushTestbed(t, core)

	resp, err := tb.client.FlushDraft(actorCtx(t, publisher()), flushReq())
	if err != nil {
		t.Fatalf("FlushDraft with no live room: %v", err)
	}
	if resp.GetRoomFound() || resp.GetContentFlushed() {
		t.Errorf("response = %+v, want both false", resp)
	}
	if got := tb.hub.RoomCount(); got != 0 {
		t.Errorf("hub room count = %d, want 0: the flush conjured a room", got)
	}
	if got := core.recorded(); len(got) != 0 {
		t.Errorf("core UpdateDraftContent calls = %d, want 0", len(got))
	}
}

func TestFlushDraft_IdleRoomIsSuccessWithNothingFlushed(t *testing.T) {
	core := &fakeCore{}
	tb := newFlushTestbed(t, core)

	tb.join(t, "draft-1", "policy-1", "u-bob")
	tb.waitForEditors(t, "draft-1", 1)

	resp, err := tb.client.FlushDraft(actorCtx(t, publisher()), flushReq())
	if err != nil {
		t.Fatalf("FlushDraft: %v", err)
	}
	if !resp.GetRoomFound() || resp.GetContentFlushed() {
		t.Errorf("response = %+v, want room_found only", resp)
	}
	if got := core.recorded(); len(got) != 0 {
		t.Errorf("core UpdateDraftContent calls = %d, want 0", len(got))
	}
}

func TestFlushDraft_CoreRejectionIsFailedPrecondition(t *testing.T) {
	core := &fakeCore{err: status.Error(codes.InvalidArgument, "content validation: boilerplate edited")}
	tb := newFlushTestbed(t, core)

	conn := tb.join(t, "draft-1", "policy-1", "u-bob")
	tb.waitForEditors(t, "draft-1", 1)
	sendCheckpoint(t, conn, lexical("latest"))

	_, err := tb.client.FlushDraft(actorCtx(t, publisher()), flushReq())
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("status code = %v, want FailedPrecondition (err: %v)", got, err)
	}
	if !strings.Contains(status.Convert(err).Message(), "boilerplate edited") {
		t.Errorf("message = %q, want core's reason", status.Convert(err).Message())
	}
}

func TestFlushDraft_CoreUnreachableIsUnavailable(t *testing.T) {
	core := &fakeCore{err: status.Error(codes.Unavailable, "connection refused")}
	tb := newFlushTestbed(t, core)

	conn := tb.join(t, "draft-1", "policy-1", "u-bob")
	tb.waitForEditors(t, "draft-1", 1)
	sendCheckpoint(t, conn, lexical("latest"))

	_, err := tb.client.FlushDraft(actorCtx(t, publisher()), flushReq())
	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("status code = %v, want Unavailable (err: %v)", got, err)
	}

	core.mu.Lock()
	core.err = nil
	core.mu.Unlock()
	resp, err := tb.client.FlushDraft(actorCtx(t, publisher()), flushReq())
	if err != nil {
		t.Fatalf("retry FlushDraft: %v", err)
	}
	if !resp.GetContentFlushed() {
		t.Error("retry did not resend the checkpoint that failed to reach core")
	}
}

func TestFlushDraft_RefusesPolicyMismatch(t *testing.T) {
	core := &fakeCore{}
	tb := newFlushTestbed(t, core)

	conn := tb.join(t, "draft-1", "policy-1", "u-bob")
	tb.waitForEditors(t, "draft-1", 1)
	sendCheckpoint(t, conn, lexical("latest"))

	_, err := tb.client.FlushDraft(actorCtx(t, publisher()),
		&collabv1.FlushDraftRequest{DraftId: "draft-1", PolicyId: "policy-somewhere-else"})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("status code = %v, want FailedPrecondition (err: %v)", got, err)
	}
	if got := core.recorded(); len(got) != 0 {
		t.Errorf("core UpdateDraftContent calls = %d, want 0", len(got))
	}
}

func TestFlushDraft_RejectsUnauthenticated(t *testing.T) {
	tb := newFlushTestbed(t, &fakeCore{})

	_, err := tb.client.FlushDraft(context.Background(), flushReq())
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("status code = %v, want Unauthenticated (err: %v)", got, err)
	}
}

func TestFlushDraft_RejectsUnusableRequests(t *testing.T) {
	tb := newFlushTestbed(t, &fakeCore{})
	ctx := actorCtx(t, publisher())

	for name, req := range map[string]*collabv1.FlushDraftRequest{
		"missing draft_id":  {PolicyId: "policy-1"},
		"missing policy_id": {DraftId: "draft-1"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := tb.client.FlushDraft(ctx, req); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("status code = %v, want InvalidArgument (err: %v)", status.Code(err), err)
			}
		})
	}
}
