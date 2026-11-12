// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	log "github.com/Bugs5382/go-log"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-collab/internal/access"
	"github.com/Steward-GRC/steward-collab/internal/ws"
)

// fakeEdit is an EditChecker whose answer a test changes between joins.
type fakeEdit struct {
	mu      sync.Mutex
	allowed bool
	err     error
	asked   [][2]string
}

func (f *fakeEdit) set(allowed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.allowed, f.err = allowed, err
}

func (f *fakeEdit) CanEdit(_ context.Context, userID, policyID string) (access.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked = append(f.asked, [2]string{userID, policyID})
	return access.Result{Allowed: f.allowed, Name: "Alice"}, f.err
}

func joinServer(t *testing.T, edit ws.EditChecker) (*ws.Hub, string) {
	t.Helper()
	hub := ws.NewHub(newMemStore(), nil, nil, log.Nop())
	go hub.Run(t.Context())
	srv := httptest.NewServer(ws.NewHandler(hub, testSecret, log.Nop()).WithEditCheck(edit))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	return hub, u.Host
}

func dialJoin(t *testing.T, host, draft string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	tok := signToken(t, validClaims(draft, "p-1", "tv-1", "u-1"), testSecret)
	return websocket.DefaultDialer.Dial("ws://"+host+"/ws/"+draft+"?token="+tok, nil)
}

func TestHandler_JoinChecksTheEditRightAndAdmitsWhoHasIt(t *testing.T) {
	edit := &fakeEdit{allowed: true}
	hub, host := joinServer(t, edit)
	conn, _, err := dialJoin(t, host, "draft-ok")
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	require.Eventually(t, func() bool { return hub.RoomCount() == 1 }, 2e9, 10e6)
	require.Equal(t, [][2]string{{"u-1", "p-1"}}, edit.asked)
}

func TestHandler_JoinIsRefusedOnceTheRightIsRemoved(t *testing.T) {
	edit := &fakeEdit{allowed: true}
	hub, host := joinServer(t, edit)
	conn, _, err := dialJoin(t, host, "draft-a")
	require.NoError(t, err)
	_ = conn.Close()

	// The token is still valid, but the user's right has gone since it was minted.
	edit.set(false, nil)
	_, resp, err := dialJoin(t, host, "draft-b")
	require.Error(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	require.Len(t, edit.asked, 2, "every join asks again")
	require.LessOrEqual(t, hub.RoomCount(), 1, "the refused join opened no room")
}

func TestHandler_JoinIsRefusedWhenThePolicyIsGone(t *testing.T) {
	_, host := joinServer(t, &fakeEdit{err: access.ErrPolicyNotFound})
	_, resp, err := dialJoin(t, host, "draft-gone")
	require.Error(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestHandler_JoinFailsClosedWhenAPeerIsDown(t *testing.T) {
	_, host := joinServer(t, &fakeEdit{err: access.ErrUnavailable})
	_, resp, err := dialJoin(t, host, "draft-down")
	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	_, host = joinServer(t, &fakeEdit{err: errors.New("boom")})
	_, resp, err = dialJoin(t, host, "draft-boom")
	require.Error(t, err)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandler_WithoutAnEditCheckTheTokenAloneAdmits(t *testing.T) {
	hub := ws.NewHub(newMemStore(), nil, nil, log.Nop())
	go hub.Run(t.Context())
	srv := httptest.NewServer(ws.NewHandler(hub, testSecret, log.Nop()))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	conn, _, err := dialJoin(t, u.Host, "draft-open")
	require.NoError(t, err)
	_ = conn.Close()
}
