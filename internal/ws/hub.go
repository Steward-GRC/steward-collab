// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws

import (
	"context"
	"sync"

	log "github.com/Bugs5382/go-log"
	"github.com/Steward-GRC/steward-collab/internal/snapshot"
	"github.com/Steward-GRC/steward-collab/internal/store"
)

// Hub manages all active Rooms (one per draft).
//
// SINGLE REPLICA ONLY. Every piece of Hub state — the room registry, each
// room's Yjs document, each room's awareness map — is process-local. Two users
// editing the same draft who land on different pods get two independent rooms:
// they never see each other's edits or cursors, and both pods race to write the
// draft snapshot. Nothing in this package detects that.
//
// collab therefore runs as one replica, with no autoscaling. Scaling out
// needs one of:
//
//   - draft-affinity routing — consistent hash on draft_id at the ingress, so a
//     given draft always terminates on the same pod; or
//   - a backplane — Redis/NATS fan-out of updates and awareness, with a single
//     owner per draft.
//
// Do not raise the replica count or re-enable the HPA before one of those ships.
type Hub struct {
	mu      sync.Mutex
	rooms   map[string]*Room // keyed by draftID
	store   store.Store
	pub     *snapshot.Publisher
	auditor SessionAuditor
	logger  log.Logger
}

// NewHub creates a Hub. auditor emits collab.session.joined/left events for
// every client that joins or leaves a room; pass nil to disable session
// auditing (e.g. in tests that do not assert on audit).
func NewHub(s store.Store, pub *snapshot.Publisher, auditor SessionAuditor, logger log.Logger) *Hub {
	return &Hub{
		rooms:   make(map[string]*Room),
		store:   s,
		pub:     pub,
		auditor: auditor,
		logger:  logger,
	}
}

// Run processes housekeeping (currently a no-op loop; placeholder for room GC).
func (h *Hub) Run(ctx context.Context) {
	<-ctx.Done()
}

// GetOrCreateRoom returns an existing Room or creates one, loading Yjs state from the store.
func (h *Hub) GetOrCreateRoom(ctx context.Context, draftID, policyID, templateVersionID string) (*Room, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if r, ok := h.rooms[draftID]; ok {
		return r, nil
	}
	doc, err := h.store.Load(ctx, draftID)
	if err != nil {
		return nil, err
	}
	var initial []byte
	if doc != nil {
		initial = doc.YjsState
	}
	r := newRoom(draftID, policyID, templateVersionID, initial, h.store, h.pub, h.auditor, h.logger)
	r.onEmpty = func() { h.removeRoom(draftID) }
	h.rooms[draftID] = r
	go r.run(ctx)
	return r, nil
}

// removeRoom deletes a room from the hub registry. Called by a room after its
// last client disconnects and its final snapshot has been flushed, so the hub
// does not keep idle rooms in memory.
//
// The removal is conditional: a new client can join during the final flush
// (which makes a gRPC call to core, so the window is not short). Dropping the
// room then would strand that client in a room the hub no longer knows about,
// and the next joiner would create a SECOND room for the same draft — two
// rooms, neither seeing the other's edits.
func (h *Hub) removeRoom(draftID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.rooms[draftID]
	if !ok || r.ClientCount() > 0 {
		return
	}
	delete(h.rooms, draftID)
}

// FlushResult reports what Hub.FlushDraft found.
type FlushResult struct {
	// RoomFound is false when nobody is editing the draft, which is not an
	// error: core already holds the newest content.
	RoomFound bool
	// ContentFlushed is true when the call sent a pending checkpoint to core.
	ContentFlushed bool
}

// FlushDraft is the publish-intent gate: it forces any pending snapshot for
// draftID to core NOW, so a draft cannot be published from content older than
// what the room holds.
//
// It blocks until core has answered, because that is the whole point: the
// caller must not proceed to PublishDraft until the flush is done. A non-nil
// error (a *FlushError) means core does not hold the room's newest content and
// the publish must not go ahead. The cross-service caller is
// CollabRoomService.FlushDraft (internal/grpcsvc/room.go).
func (h *Hub) FlushDraft(ctx context.Context, draftID string) (FlushResult, error) {
	room := h.room(draftID)
	if room == nil {
		return FlushResult{}, nil
	}
	flushed, err := room.Flush(ctx)
	return FlushResult{RoomFound: true, ContentFlushed: flushed}, err
}

// MarkDraftPublished drops the draft's room to read-only, flushes anything
// still pending, and pushes a draft.published control frame to every connected
// editor. Call it AFTER core confirms the publish (FlushDraft is the call that
// belongs before it). Reports whether a room existed.
//
// The cross-service caller is CollabRoomService.NotifyDraftPublished
// (internal/grpcsvc/room.go), which the gateway's publishDraft resolver invokes
// once core has confirmed the publish.
func (h *Hub) MarkDraftPublished(ctx context.Context, draftID string, versionNumber int32) bool {
	room := h.room(draftID)
	if room == nil {
		return false
	}
	room.MarkPublished(ctx, versionNumber)
	return true
}

// room returns the live room for draftID, or nil. It never creates one: a
// publish-intent must not conjure a room for a draft nobody is editing.
func (h *Hub) room(draftID string) *Room {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.rooms[draftID]
}

// RoomInfo is a read-only description of a live room, for callers outside this
// package that need to reason about a room without being handed the room
// itself (and with it the ability to mutate the document).
type RoomInfo struct {
	// PolicyID is the policy the room's draft belongs to, taken from the token
	// the first joiner presented.
	PolicyID string
	// TemplateVersionID is the TemplateVersion the room's snapshots validate
	// against.
	TemplateVersionID string
	// Editors is the number of clients currently connected.
	Editors int
}

// LookupRoom returns metadata for the live room for draftID and whether one
// exists at all. Like room, it NEVER creates a room: "nobody is editing this
// draft" must stay distinguishable from "a room exists", because that is the
// normal case for an out-of-band publish notification.
//
// It exists so CollabRoomService can check a caller's asserted policy id
// against the room's own before freezing it, without exporting *Room across
// the package boundary.
func (h *Hub) LookupRoom(draftID string) (RoomInfo, bool) {
	room := h.room(draftID)
	if room == nil {
		return RoomInfo{}, false
	}
	// policyID / templateVersionID are set at construction and never mutated,
	// so they need no lock; ClientCount reads an atomic.
	return RoomInfo{
		PolicyID:          room.policyID,
		TemplateVersionID: room.templateVersionID,
		Editors:           room.ClientCount(),
	}, true
}

// RoomCount returns the number of active rooms (intended for tests / metrics).
func (h *Hub) RoomCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.rooms)
}
