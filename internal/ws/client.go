// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws

import (
	"context"
	"sync/atomic"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/gorilla/websocket"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
	// maxMessageSize caps one inbound frame. It has to accommodate the largest
	// legitimate message, which is now MsgSnapshot: a full Lexical EditorState
	// JSON export PLUS the client's compacted Yjs state for the same document,
	// in one frame. The old 512 KB budget was sized for incremental Yjs updates
	// only; a long policy with appendices can exceed it once both full copies
	// ride together, and gorilla closes the connection on an oversized frame —
	// which would make exactly the largest drafts unable to persist.
	//
	// CHANGING THIS VALUE IS A TWO-REPO CHANGE. The gateway's collab WS proxy
	// caps relayed frames and must stay strictly above this limit, but cannot
	// import it (unexported, internal/, and the gateway depends on
	// collab's proto only). It mirrors it as collabws.CollabReadLimitBytes.
	// Raising this alone makes the proxy the tighter of the two, which closes
	// large snapshots with 1009.
	// TestMaxMessageSize_MatchesGatewayProxyMirror fails if the two drift.
	maxMessageSize = 2 * 1024 * 1024 // 2 MiB
)

// Client represents a single connected websocket peer.
type Client struct {
	room        *Room
	conn        *websocket.Conn
	send        chan []byte
	userID      string
	displayName string
	// impersonator is the token's real admin during act-as, otherwise empty.
	impersonator string
	logger       log.Logger

	// resync is set when a document (MsgSync) frame had to be dropped because
	// this client's send buffer was full. A dropped sync frame is unrecoverable
	// on its own — Yjs has no gap detection, so the peer would stay silently
	// behind forever — so the write pump repairs it as soon as the buffer
	// drains. Awareness frames are exempt: they are transient and re-sent.
	resync atomic.Bool

	// awarenessIDs is the set of Yjs client ids this connection has announced
	// presence for. It is written and read only from the read pump goroutine
	// (handleMessage and its deferred teardown), so it needs no lock. On
	// disconnect the room clears these so peers drop the cursor immediately.
	awarenessIDs map[uint32]struct{}
}

// NewClient creates a Client and starts its read and write pumps. displayName
// is the collab token's `name` claim (the Gateway resolves it from the caller's
// identity record); it is the server-authoritative presence label and falls
// back to userID when empty.
func NewClient(ctx context.Context, room *Room, conn *websocket.Conn, userID, displayName string, logger log.Logger) *Client {
	return NewImpersonatedClient(ctx, room, conn, userID, displayName, "", logger)
}

// NewImpersonatedClient is NewClient for a token minted during act-as:
// impersonator is the real admin, credited in audit and carried to core with
// every snapshot. Empty is the same as NewClient.
func NewImpersonatedClient(ctx context.Context, room *Room, conn *websocket.Conn, userID, displayName, impersonator string, logger log.Logger) *Client {
	c := &Client{
		impersonator: impersonator,
		room:         room,
		conn:         conn,
		send:         make(chan []byte, 128),
		userID:       userID,
		displayName:  displayName,
		logger:       logger,
		awarenessIDs: make(map[uint32]struct{}),
	}
	room.Register(c)
	go c.writePump()
	go c.readPump(ctx)
	return c
}

func (c *Client) readPump(ctx context.Context) {
	defer func() {
		// Clear this peer's presence before unregistering so the remaining
		// clients see the cursor disappear at once rather than waiting out the
		// client-side awareness timeout.
		c.room.releaseAwareness(c.awarenessIDs)
		c.room.Unregister(c)
		_ = c.conn.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// Open the handshake the way y-websocket's server does: MsgSync/SyncStep1
	// carrying OUR state vector.
	//
	// It must be a state vector, not the document. The relay keeps no Y.Doc, so
	// the only honest state vector it can offer is the empty one — "I know of no
	// client clocks, send me everything". A Yjs peer answers that with a
	// SyncStep2 carrying its full document. Handing it the stored document blob
	// instead (the previous behaviour) is read by `readStateVector` as an
	// arbitrary set of client clocks, so the peer withholds the very updates the
	// room is missing.
	//
	// All writes must go through the send channel so writePump is the sole writer.
	c.send <- EncodeSync1(EmptyStateVector())

	// Watch for context cancellation: closing the underlying conn unblocks ReadMessage.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = c.conn.Close()
		case <-done:
		}
	}()

	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.logger.Warn("ws read error", log.F("error", err.Error()), log.F("user_id", c.userID))
			}
			return
		}
		c.handleMessage(ctx, msg)
	}
}

func (c *Client) handleMessage(ctx context.Context, msg []byte) {
	msgType, payload, err := DecodeMessage(msg)
	if err != nil {
		c.logger.Warn("decode message", log.F("error", err.Error()))
		return
	}
	switch msgType {
	case MsgSync:
		syncType, data, err := DecodeSyncMessage(payload)
		if err != nil {
			c.logger.Warn("decode sync message", log.F("error", err.Error()))
			return
		}
		switch syncType {
		case SyncStep1:
			// Client sent its state vector; respond with our diff (full state
			// for now — the relay cannot compute a diff). Route through the send
			// channel so writePump is the sole writer; a full buffer schedules a
			// resync instead of dropping the peer's only copy of the document.
			c.queueSync(EncodeSync2(c.room.DocumentUpdate()))
		case SyncStep2, SyncUpdate:
			// Client sent an update; apply it and fan out.
			c.room.ApplyUpdate(ctx, data)
		}
	case MsgAwareness:
		body, err := DecodeAwarenessMessage(payload)
		if err != nil {
			c.logger.Warn("decode awareness message", log.F("error", err.Error()), log.F("user_id", c.userID))
			return
		}
		// Bind the awareness identity to this connection's authenticated user
		// before fanning it out, so a client cannot spoof another user's
		// cursor/name/color. The server is authoritative for
		// the `user` field; the client's asserted uid/name/color is ignored.
		bound, entries, err := bindAwarenessIdentity(body, newAwarenessUser(c.userID, c.displayName))
		if err != nil {
			c.logger.Warn("bind awareness identity", log.F("error", err.Error()), log.F("user_id", c.userID))
			return
		}
		c.trackAwareness(entries)
		c.room.recordAwareness(entries)
		c.room.broadcastAwareness(EncodeAwareness(bound))
	case MsgSnapshot:
		// The checkpoint that makes live editing durable: the Lexical JSON goes
		// to core (authoritative content) and the compacted Yjs state becomes
		// the room's rehydration copy. The acting user is THIS connection's
		// authenticated JWT uid — never a client-asserted value — so core's
		// audit row names the human who was editing.
		contentJSON, yjsState, err := DecodeSnapshotMessage(payload)
		if err != nil {
			c.logger.Warn("decode snapshot message", log.F("error", err.Error()), log.F("user_id", c.userID))
			return
		}
		c.room.recordSnapshot(ctx, contentJSON, yjsState, c.userID, c.impersonator)
	case MsgQueryAwareness:
		// A stock y-websocket client sends this on connect. Left unanswered, a
		// joiner believes it is alone in the room until an existing peer next
		// moves their cursor. The frame carries no payload.
		if frame := c.room.AwarenessState(); frame != nil {
			c.queueAwareness(frame)
		}
	default:
		// MsgAuth and MsgControl are server→client only; anything else is a
		// protocol the relay does not speak.
		c.logger.Debug("unhandled collab message type", log.F("msg_type", msgType), log.F("user_id", c.userID))
	}
}

// trackAwareness remembers which Yjs client ids this connection has announced,
// so its presence can be cleared when it disconnects. Only ever called from the
// read pump goroutine.
func (c *Client) trackAwareness(entries []awarenessEntry) {
	for _, e := range entries {
		if e.cleared() {
			delete(c.awarenessIDs, e.ClientID)
			continue
		}
		c.awarenessIDs[e.ClientID] = struct{}{}
	}
}

// queueSync hands a document frame to the write pump. Unlike awareness, a
// dropped sync frame cannot be recovered by the peer on its own, so an
// overflowing buffer flags the client for resync rather than discarding the
// update silently.
func (c *Client) queueSync(msg []byte) {
	select {
	case c.send <- msg:
	default:
		c.resync.Store(true)
		c.logger.Warn("send buffer full; dropped sync frame, scheduling resync", log.F("user_id", c.userID), log.F("draft_id", c.room.draftID))
	}
}

// queueAwareness hands a presence frame to the write pump. Unlike a document
// frame, a dropped awareness frame is safe to discard and deliberately does NOT
// flag the client for resync: presence is transient, every peer re-advertises
// it on its next change, and stale entries time out client-side.
func (c *Client) queueAwareness(frame []byte) {
	select {
	case c.send <- frame:
	default:
		// Transient; the next presence change re-advertises it.
	}
}

// queueControl hands a MsgControl frame to the write pump. A dropped control
// frame is NOT recoverable by the peer (nothing re-advertises it) and must not
// trigger a document resync either — re-sending the document does not tell the
// editor its snapshot was refused — so the drop is logged loudly instead.
func (c *Client) queueControl(frame []byte) {
	select {
	case c.send <- frame:
	default:
		c.logger.Warn("send buffer full; dropped control frame", log.F("user_id", c.userID), log.F("draft_id", c.room.draftID))
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() { ticker.Stop(); _ = c.conn.Close() }()
	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.write(websocket.BinaryMessage, msg); err != nil {
				return
			}
			if err := c.repairIfStale(); err != nil {
				return
			}
		case <-ticker.C:
			if err := c.write(websocket.PingMessage, nil); err != nil {
				return
			}
			// Also checked here so a client that fell behind and then went quiet
			// still gets repaired, at worst one ping period later.
			if err := c.repairIfStale(); err != nil {
				return
			}
		}
	}
}

// write sends one frame, refreshing the write deadline. writePump is the sole
// writer of the connection, so only it may call this.
func (c *Client) write(messageType int, msg []byte) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	return c.conn.WriteMessage(messageType, msg)
}

// repairIfStale re-sends the room's document state to a client that had a sync
// frame dropped (see queueSync). It writes directly rather than through the
// send channel: the channel is exactly what just overflowed, and writePump is
// the sole writer so a direct write is safe.
//
// A SyncStep2 carrying the room's state is the repair a relay can make on its
// own — Yjs applies updates idempotently and commutatively, so re-delivering
// state the peer already has is harmless. No SyncStep1 is issued: that would ask
// the peer to re-upload its whole document, which the room does not need in
// order to fill this peer's gap.
func (c *Client) repairIfStale() error {
	if !c.resync.CompareAndSwap(true, false) {
		return nil
	}
	c.logger.Info("resyncing collab client after dropped sync frame", log.F("user_id", c.userID), log.F("draft_id", c.room.draftID))
	return c.write(websocket.BinaryMessage, EncodeSync2(c.room.DocumentUpdate()))
}
