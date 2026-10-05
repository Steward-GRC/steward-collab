// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/Steward-GRC/steward-collab/internal/audit"
	"github.com/Steward-GRC/steward-collab/internal/snapshot"
	"github.com/Steward-GRC/steward-collab/internal/store"
	"google.golang.org/grpc/codes"
)

const snapshotDebounce = 5 * time.Second

// SessionAuditor emits collab session lifecycle audit events
// (collab.session.joined / collab.session.left). *audit.Emitter satisfies it;
// tests supply a fake. A nil auditor disables session auditing.
type SessionAuditor interface {
	Emit(ctx context.Context, ev audit.Event) error
}

// Room holds one draft's collaboration session: the clients connected to it,
// the presence map, and the last CLIENT-COMPACTED Yjs state.
//
// # Pure relay — the server runs no Y.Doc
//
// The room never decodes, applies or merges Yjs. Updates are fanned out to
// peers verbatim; convergence is the clients' business (that is what a CRDT
// buys). The room's yjsState is therefore NOT derived from the update stream —
// it is whatever full state a client last handed over in a MsgSnapshot
// (`Y.encodeStateAsUpdate(doc)`), stored verbatim and served back on
// rehydration.
//
// This is safe because the AUTHORITATIVE content is the Lexical JSON that rides
// the same MsgSnapshot frame and is validated by core on UpdateDraftContent
// (draft status, pinned template_version_id, template-section shape). The Yjs
// blob is a resume convenience, not a source of truth: if it were lost or
// corrupt, a fresh room can be reseeded from core's draft content.
//
// It also fixes the previous behaviour, which concatenated every update onto
// yjsState and rewrote the row on every batch. Concatenated updates are not a
// valid Yjs update — a rehydrated client decoded only the FIRST update ever
// applied and silently discarded the rest — and the column grew without bound
// for the life of the draft.
type Room struct {
	draftID           string
	policyID          string
	templateVersionID string

	mu sync.RWMutex
	// yjsState is the last client-compacted full document state. It changes
	// only on snapshot, never per update.
	yjsState []byte
	// pending is the checkpoint waiting for the debounce to fire, or nil when
	// no client snapshot has arrived since the last flush.
	pending *pendingSnapshot
	// published latches when the draft has been published. The version is then
	// immutable, so the room stops accepting updates and snapshots.
	published bool

	// flushMu serializes flushes. Without it a publish-time Flush that finds
	// nothing pending would return while a debounced flush still has the
	// newest checkpoint in flight to core. It also guards lastOutcome.
	flushMu sync.Mutex
	// lastOutcome is core's answer to the most recent checkpoint sent, or nil
	// before the first. A rejection stands until a newer checkpoint is
	// accepted, because until then core does not hold the room's content.
	lastOutcome *snapshot.Outcome

	register   chan *Client
	unregister chan *Client
	broadcast  chan []byte // raw update bytes to fanout
	// awareness carries already-encoded MsgAwareness frames to fan out as-is.
	// Awareness is a distinct wire message from a Yjs sync update, so it must
	// NOT go through the broadcast channel (which wraps its payload in an
	// EncodeUpdate sync frame) — that would misroute presence as document data.
	awareness chan []byte
	// control carries already-encoded MsgControl frames (snapshot outcome,
	// draft published) to fan out as-is, for the same reason as awareness.
	control chan []byte

	// awarenessMu guards awarenessStates, the room's server-side view of who is
	// present. It is deliberately separate from mu (which guards the document)
	// so presence traffic never contends with the Yjs state lock.
	//
	// A stock y-websocket client sends MsgQueryAwareness immediately on connect
	// and expects the room's current presence back. Without this map the server
	// has nothing to answer with, so a joiner believes it is alone until some
	// existing peer happens to move their cursor. Keyed by Yjs client id — one
	// user can hold several (two tabs), which is why the key is not the uid.
	awarenessMu     sync.Mutex
	awarenessStates map[uint32]awarenessEntry

	clients map[*Client]struct{}
	// clientCount mirrors len(clients) for readers outside the room goroutine
	// (metrics, and tests that must not race the registration they depend on).
	// The map itself is only ever touched by run.
	clientCount atomic.Int64
	store       store.Store
	pub         *snapshot.Publisher
	auditor     SessionAuditor
	logger      log.Logger

	// debouncer coalesces a burst of client snapshots into one flush. It is
	// snapshot.Debouncer rather than a hand-rolled time.Timer so there is a
	// single debounce implementation in the service, and because flushing on
	// last-collaborator-leaves needs its Stop.
	debouncer *snapshot.Debouncer

	// onEmpty is invoked when the last client leaves so the owning Hub can drop
	// its reference to this room. It runs after the final snapshot flush.
	onEmpty func()
}

// pendingSnapshot is one client checkpoint awaiting the debounced flush.
type pendingSnapshot struct {
	// contentJSON is the Lexical EditorState JSON destined for core.
	contentJSON string
	// yjsState is the client's compacted full document state, stored verbatim.
	yjsState []byte
	// actorUserID is the token user of the connection that sent the snapshot,
	// so core's audit row names the person rather than the collab service.
	actorUserID string
	// impersonator is the token's real admin during act-as.
	impersonator string
}

func newRoom(draftID, policyID, templateVersionID string, initial []byte,
	s store.Store, pub *snapshot.Publisher, auditor SessionAuditor, logger log.Logger) *Room {
	return &Room{
		draftID:           draftID,
		policyID:          policyID,
		templateVersionID: templateVersionID,
		yjsState:          initial,
		register:          make(chan *Client, 8),
		unregister:        make(chan *Client, 8),
		broadcast:         make(chan []byte, 64),
		awareness:         make(chan []byte, 64),
		control:           make(chan []byte, 16),
		awarenessStates:   make(map[uint32]awarenessEntry),
		clients:           make(map[*Client]struct{}),
		store:             s,
		pub:               pub,
		auditor:           auditor,
		logger:            logger,
		debouncer:         snapshot.NewDebouncer(snapshotDebounce),
	}
}

// CurrentState returns a copy of the Yjs binary state under a read lock.
func (r *Room) CurrentState() []byte {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cp := make([]byte, len(r.yjsState))
	copy(cp, r.yjsState)
	return cp
}

// DocumentUpdate returns the room's Yjs state as a payload that is safe to put
// in a Sync Step-2 or Update frame: the stored state, or the canonical
// empty-document update when the room has none yet. A zero-length payload is
// not valid Yjs — see EmptyDocumentUpdate.
func (r *Room) DocumentUpdate() []byte {
	if state := r.CurrentState(); len(state) > 0 {
		return state
	}
	return EmptyDocumentUpdate()
}

// ApplyUpdate relays a Yjs update to every peer in the room.
//
// It deliberately does NOT touch r.yjsState and does NOT write to the store.
// The relay owns no Y.Doc, so it cannot produce a valid merged state from a
// stream of updates; the compacted state arrives on MsgSnapshot instead (see
// RecordSnapshot). Saving per update was also what made collab_documents grow
// without bound.
func (r *Room) ApplyUpdate(_ context.Context, update []byte) {
	// A zero-length update carries nothing, and fanning one out would make every
	// peer's Y.applyUpdate throw. Drop it rather than propagate it.
	if len(update) == 0 {
		return
	}
	// A published version is immutable: refuse to relay further edits rather
	// than spread changes that can never be persisted.
	if r.isPublished() {
		return
	}
	r.broadcast <- update
}

// RecordSnapshot stores a client checkpoint and arms the debounced flush. It is
// the entry point for the MsgSnapshot frame and therefore the only path by
// which live edits reach durable storage.
//
// contentJSON is the Lexical EditorState JSON destined for core; yjsState is
// the client's compacted `Y.encodeStateAsUpdate(doc)`, stored verbatim (empty
// means "keep the state already held"). actorUserID is the connection's
// authenticated JWT uid, never a client-asserted value.
//
// Only a snapshot arms the debouncer. An update alone has nothing to flush —
// the publisher needs Lexical JSON, which only a client can produce from the
// Yjs document — so arming on update just burned a timer.
func (r *Room) RecordSnapshot(ctx context.Context, contentJSON string, yjsState []byte, actorUserID string) {
	r.recordSnapshot(ctx, contentJSON, yjsState, actorUserID, "")
}

// recordSnapshot is RecordSnapshot with the act-as admin of the connection.
func (r *Room) recordSnapshot(ctx context.Context, contentJSON string, yjsState []byte, actorUserID, impersonator string) {
	if contentJSON == "" && len(yjsState) == 0 {
		return
	}
	if r.isPublished() {
		return
	}
	r.mu.Lock()
	r.pending = &pendingSnapshot{
		contentJSON:  contentJSON,
		yjsState:     append([]byte(nil), yjsState...),
		actorUserID:  actorUserID,
		impersonator: impersonator,
	}
	r.mu.Unlock()
	r.debouncer.Trigger(func() { r.flushSnapshot(ctx) })
}

// FlushSnapshot cancels any pending debounce and flushes immediately. It is the
// serialization point used when the last collaborator leaves (otherwise the
// final edits die with the room) and on publish. Returns true if there was
// something to flush. A publish uses Flush instead, which also reports core's
// verdict.
func (r *Room) FlushSnapshot(ctx context.Context) bool {
	r.debouncer.Stop()
	return r.flushSnapshot(ctx)
}

// FlushError reports that core does not hold the room's newest checkpoint.
// Code is core's gRPC status code when core answered with an error, and OK
// when the checkpoint never reached core or core returned accepted=false.
type FlushError struct {
	Reason string
	Detail string
	Code   codes.Code
}

func (e *FlushError) Error() string {
	return "checkpoint not accepted by core: " + e.Reason + ": " + e.Detail
}

// Flush is the publish-time flush: it sends any pending checkpoint to core and
// returns once core has answered, waiting out a debounced flush already in
// flight. The error is non-nil whenever core does not hold the newest
// checkpoint the room has received, including a rejection from an earlier
// flush that no newer checkpoint has replaced. flushed reports whether this
// call sent a checkpoint.
func (r *Room) Flush(ctx context.Context) (flushed bool, err error) {
	r.debouncer.Stop()
	r.flushMu.Lock()
	defer r.flushMu.Unlock()
	flushed = r.flushPendingLocked(ctx)
	if o := r.lastOutcome; o != nil && !o.Accepted {
		return flushed, &FlushError{Reason: o.Reason, Detail: o.Detail, Code: o.Code}
	}
	return flushed, nil
}

// flushSnapshot flushes the pending checkpoint under flushMu. Returns false
// when there was nothing pending.
func (r *Room) flushSnapshot(ctx context.Context) bool {
	r.flushMu.Lock()
	defer r.flushMu.Unlock()
	return r.flushPendingLocked(ctx)
}

// flushPendingLocked takes the pending checkpoint, adopts and persists its
// compacted Yjs state, forwards its Lexical JSON to core, and tells the room's
// clients what core decided. The caller holds flushMu.
func (r *Room) flushPendingLocked(ctx context.Context) bool {
	r.mu.Lock()
	p := r.pending
	r.pending = nil
	if p != nil && len(p.yjsState) > 0 {
		// Adopt the client's compacted state unconditionally — even if core
		// then rejects the Lexical JSON. The peers' live documents already
		// contain these changes, so the room must be able to rehydrate to the
		// state everyone is actually editing; core's rejection is reported to
		// the editor over the control channel instead.
		r.yjsState = p.yjsState
	}
	r.mu.Unlock()
	if p == nil {
		return false
	}

	if r.store != nil && len(p.yjsState) > 0 {
		if err := r.store.Save(ctx, store.Document{
			DraftID:           r.draftID,
			PolicyID:          r.policyID,
			TemplateVersionID: r.templateVersionID,
			YjsState:          p.yjsState,
		}); err != nil {
			r.logger.Error(err, "store save", log.F("draft_id", r.draftID))
		}
	}

	if r.pub == nil || p.contentJSON == "" {
		return true
	}
	outcome := r.pub.SnapshotJSON(ctx, snapshot.Checkpoint{
		DraftID:           r.draftID,
		PolicyID:          r.policyID,
		TemplateVersionID: r.templateVersionID,
		ContentJSON:       p.contentJSON,
		ActorUserID:       p.actorUserID,
		Impersonator:      p.impersonator,
	})
	r.lastOutcome = &outcome
	if !outcome.Accepted && isTransient(outcome.Code) {
		r.requeue(p)
	}
	r.notifySnapshotOutcome(outcome)
	return true
}

// isTransient reports whether a failed checkpoint says nothing about the
// content itself, so resending the same checkpoint may succeed.
func isTransient(c codes.Code) bool {
	switch c {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return true
	}
	return false
}

// requeue puts a checkpoint that failed transiently back as pending, unless a
// newer one has arrived meanwhile or the room is already published.
func (r *Room) requeue(p *pendingSnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending == nil && !r.published {
		r.pending = p
	}
}

// notifySnapshotOutcome tells every client in the room what core did with the
// checkpoint. Without this a rejected snapshot is invisible: the room keeps
// running and the editor keeps accepting keystrokes that will never persist.
func (r *Room) notifySnapshotOutcome(o snapshot.Outcome) {
	msg := ControlMessage{Type: ControlSnapshotAccepted, DraftID: r.draftID}
	if !o.Accepted {
		msg = ControlMessage{
			Type:    ControlSnapshotRejected,
			DraftID: r.draftID,
			Reason:  o.Reason,
			Detail:  o.Detail,
		}
	}
	r.broadcastControl(msg)
}

// MarkPublished closes a published draft's room: it latches read-only, flushes
// anything still pending, and pushes a draft.published control frame so every
// editor drops to read-only instead of typing into a version that can no longer
// change.
//
// Read-only is latched BEFORE the flush so no further content can arrive during
// it. Publish is therefore two-phase, and the phases are separate on purpose:
// Flush (via Hub.FlushDraft) runs BEFORE core cuts the version, so the
// version is cut from the room's newest content; MarkPublished runs AFTER core
// confirms, so a failed publish does not leave a live room stuck read-only.
//
// The cross-service seam is CollabRoomService.NotifyDraftPublished, called by
// the gateway once core has confirmed the publish (see Hub.MarkDraftPublished).
func (r *Room) MarkPublished(ctx context.Context, versionNumber int32) {
	r.mu.Lock()
	r.published = true
	r.mu.Unlock()
	r.FlushSnapshot(ctx)
	r.broadcastControl(ControlMessage{
		Type:          ControlDraftPublished,
		DraftID:       r.draftID,
		VersionNumber: versionNumber,
	})
}

// isPublished reports whether the draft has been published and the room is
// therefore read-only.
func (r *Room) isPublished() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.published
}

func (r *Room) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case c := <-r.register:
			// Ask the peers already here to re-advertise their document. The
			// relay holds only the last COMPACTED state, so anything typed
			// since the previous snapshot lives solely in the peers' Y.Docs;
			// each answers this SyncStep1 with a SyncStep2 carrying its full
			// document, which fans out to the joiner. Without it a mid-session
			// joiner silently starts from stale content.
			for peer := range r.clients {
				peer.queueSync(EncodeSync1(EmptyStateVector()))
			}
			r.clients[c] = struct{}{}
			r.clientCount.Store(int64(len(r.clients)))
			r.emitSession(ctx, "collab.session.joined", c.userID, c.impersonator)
		case c := <-r.unregister:
			if _, ok := r.clients[c]; !ok {
				continue
			}
			delete(r.clients, c)
			r.clientCount.Store(int64(len(r.clients)))
			close(c.send)
			r.emitSession(ctx, "collab.session.left", c.userID, c.impersonator)
			if len(r.clients) == 0 {
				// Last collaborator out: flush before the hub forgets the room,
				// or every edit since the previous debounce is lost. Off the
				// room goroutine so a slow core RPC cannot stall the room, and
				// onEmpty runs only afterwards so the hub cannot drop the room
				// mid-flush.
				go func() {
					r.FlushSnapshot(ctx)
					if r.onEmpty != nil {
						r.onEmpty()
					}
				}()
			}
		case update := <-r.broadcast:
			msg := EncodeUpdate(update)
			for c := range r.clients {
				// queueSync, NOT a silent drop: a discarded document frame
				// desyncs that client forever (Yjs has no gap detection and
				// never re-asks), so an overflowing buffer schedules a resync.
				c.queueSync(msg)
			}
		case frame := <-r.awareness:
			// Awareness frames are already fully encoded (MsgAwareness) and must
			// be fanned out verbatim — never wrapped in an update frame.
			for c := range r.clients {
				c.queueAwareness(frame)
			}
		case frame := <-r.control:
			// Control frames are already fully encoded (MsgControl); same
			// reasoning as awareness — never wrap them in an update frame.
			for c := range r.clients {
				c.queueControl(frame)
			}
		}
	}
}

// broadcastAwareness queues a fully-encoded MsgAwareness frame for fan-out to
// all clients in the room. Non-blocking: if the awareness buffer is full the
// frame is dropped, which — unlike a document frame — is safe, because presence
// is transient and every peer re-advertises it on its next change (and lets it
// time out otherwise).
func (r *Room) broadcastAwareness(frame []byte) {
	select {
	case r.awareness <- frame:
	default:
		// buffer full; drop this transient presence update
	}
}

// broadcastControl encodes msg and queues it for fan-out to every client in the
// room. A full buffer is logged rather than silently swallowed: unlike presence
// a control frame is never re-advertised, and a lost snapshot.rejected leaves
// the editor believing its edits are safe.
func (r *Room) broadcastControl(msg ControlMessage) {
	frame, err := EncodeControl(msg)
	if err != nil {
		r.logger.Error(err, "encode control frame", log.F("draft_id", r.draftID), log.F("type", msg.Type))
		return
	}
	select {
	case r.control <- frame:
	default:
		r.logger.Warn("control buffer full; dropped control frame", log.F("draft_id", r.draftID), log.F("type", msg.Type))
	}
}

// recordAwareness folds an inbound (already identity-bound) awareness update
// into the room's presence map so MsgQueryAwareness can be answered. Entries
// with a stale clock are ignored and a cleared entry removes the peer, matching
// y-protocols/awareness.js applyAwarenessUpdate.
func (r *Room) recordAwareness(entries []awarenessEntry) {
	r.awarenessMu.Lock()
	defer r.awarenessMu.Unlock()
	for _, e := range entries {
		if prev, ok := r.awarenessStates[e.ClientID]; ok && e.Clock < prev.Clock {
			continue
		}
		if e.cleared() {
			delete(r.awarenessStates, e.ClientID)
			continue
		}
		r.awarenessStates[e.ClientID] = e
	}
}

// AwarenessState returns a fully-encoded MsgAwareness frame describing every
// peer currently present in the room, or nil when there is nobody to report.
// This is the reply to a client's MsgQueryAwareness.
func (r *Room) AwarenessState() []byte {
	r.awarenessMu.Lock()
	entries := make([]awarenessEntry, 0, len(r.awarenessStates))
	for _, e := range r.awarenessStates {
		entries = append(entries, e)
	}
	r.awarenessMu.Unlock()
	if len(entries) == 0 {
		return nil
	}
	sortAwarenessEntries(entries)
	return EncodeAwareness(encodeAwarenessUpdate(entries))
}

// releaseAwareness drops every Yjs client id a departing peer had announced and
// fans out a cleared-state update for them, so the remaining peers remove the
// cursor immediately instead of waiting out the client-side awareness timeout
// (and so the room's presence map cannot grow without bound).
func (r *Room) releaseAwareness(clientIDs map[uint32]struct{}) {
	if len(clientIDs) == 0 {
		return
	}
	r.awarenessMu.Lock()
	cleared := make([]awarenessEntry, 0, len(clientIDs))
	for id := range clientIDs {
		prev, ok := r.awarenessStates[id]
		if !ok {
			continue
		}
		delete(r.awarenessStates, id)
		// A cleared entry must advance the clock, or peers treat it as stale.
		cleared = append(cleared, awarenessEntry{
			ClientID: id,
			Clock:    prev.Clock + 1,
			State:    []byte(awarenessStateCleared),
		})
	}
	r.awarenessMu.Unlock()
	if len(cleared) == 0 {
		return
	}
	sortAwarenessEntries(cleared)
	r.broadcastAwareness(EncodeAwareness(encodeAwarenessUpdate(cleared)))
}

// emitSession publishes a collab session lifecycle audit event with the actor
// bound to the token user and the subject scoped to the policy. During act-as
// the event names the real admin.
// Emission is best-effort and off the room goroutine so a slow/broken audit
// broker never stalls presence fan-out.
func (r *Room) emitSession(ctx context.Context, action, uid, impersonator string) {
	if r.auditor == nil || uid == "" {
		return
	}
	policyID, draftID := r.policyID, r.draftID
	go func() {
		ev := audit.Event{
			Tier:        audit.TierAudit,
			Action:      action,
			ActorUserID: uid,
			Subject:     "policy:" + policyID,
			Attributes: map[string]string{
				"policy_id": policyID,
				"draft_id":  draftID,
			},
		}
		if impersonator != "" {
			ev = ev.ActedAs(impersonator)
		}
		if err := r.auditor.Emit(ctx, ev); err != nil {
			r.logger.Warn("emit collab session audit", log.F("error", err.Error()), log.F("action", action), log.F("user_id", uid))
		}
	}()
}

// ClientCount returns how many clients are currently registered in the room.
func (r *Room) ClientCount() int { return int(r.clientCount.Load()) }

// Register adds a client to the room.
func (r *Room) Register(c *Client) { r.register <- c }

// Unregister removes a client from the room.
func (r *Room) Unregister(c *Client) { r.unregister <- c }
