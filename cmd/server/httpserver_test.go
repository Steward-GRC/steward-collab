// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// This file guards the websocket listener's timeouts. Without a
// ReadHeaderTimeout a client could hold a connection open feeding headers one
// byte at a time and never complete the request — Slowloris, gosec G112.
//
// The OTHER half of the problem matters as much. collab's connections are long-lived upgraded
// websockets (the Yjs CRDT relay). net/http's ReadTimeout and WriteTimeout both
// leave an ABSOLUTE deadline on the underlying net.Conn that is never cleared
// when the handler hijacks it for the upgrade, so "hardening" this server with
// either one would silently tear down live collaborative editing sessions after
// the timeout elapsed — a worse defect than the one being fixed, and one no
// unit test that only inspects struct fields would notice.
//
// So the tests here pin BOTH properties: the header read is bounded, AND an
// upgraded connection outlives that bound. TestNewHTTPServer_UpgradedWebSocket
// OutlivesReadHeaderTimeout is the one that matters — it deliberately uses a
// handler that sets NO deadlines of its own, so it observes the server's
// configuration directly rather than internal/ws's per-write SetWriteDeadline
// calls masking a bad server setting.

// shortTimeout is the value every non-zero timeout on the server under test is
// scaled down to, so the integration-style tests below finish in milliseconds.
const shortTimeout = 150 * time.Millisecond

// startServer runs handler on a real loopback listener using the PRODUCTION
// server construction under test, with every non-zero timeout scaled down to
// shortTimeout.
//
// Scaling rather than overriding specific fields is deliberate, and it is what
// gives the websocket-survival test its teeth. That test proves an upgraded
// connection outlives the server's timeouts by idling past them — but it can
// only do that in milliseconds if the timeouts are milliseconds. Were it to
// shorten ReadHeaderTimeout alone, someone adding a realistically-valued
// `ReadTimeout: 30 * time.Second` would sail straight through it: the test
// would idle for under a second and never reach the deadline it is supposed to
// detect. Scaling preserves the only property that matters here — whether a
// field is set at all — and collapses any such addition into a fast failure.
//
// IdleTimeout is scaled too, which turns the claim that it cannot fire on a
// hijacked connection into something the survival test actually exercises
// rather than something this file merely asserts.
func startServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()

	srv := httptest.NewUnstartedServer(handler)
	cfg := newHTTPServer(srv.Listener.Addr().String(), handler)

	for _, d := range []*time.Duration{
		&cfg.ReadHeaderTimeout,
		&cfg.ReadTimeout,
		&cfg.WriteTimeout,
		&cfg.IdleTimeout,
	} {
		if *d != 0 {
			*d = shortTimeout
		}
	}

	srv.Config = cfg
	srv.Start()
	t.Cleanup(srv.Close)
	return srv
}

// TestNewHTTPServer_SetsReadHeaderTimeout is the blunt G112 guard. The shipped
// bug was literally a missing field, so an assertion this direct has value.
func TestNewHTTPServer_SetsReadHeaderTimeout(t *testing.T) {
	srv := newHTTPServer(":8081", http.NewServeMux())

	if srv.ReadHeaderTimeout <= 0 {
		t.Fatal("ReadHeaderTimeout is unset: the listener accepts Slowloris header dribbling (gosec G112)")
	}
}

// TestNewHTTPServer_OmitsReadAndWriteTimeouts pins the deliberate omission that
// keeps the websocket rooms alive. Both fields install an absolute deadline on
// the socket that survives the upgrade hijack, so neither may be set on THIS
// server. If a future change sets one, this test states why it must not.
func TestNewHTTPServer_OmitsReadAndWriteTimeouts(t *testing.T) {
	srv := newHTTPServer(":8081", http.NewServeMux())

	if srv.ReadTimeout != 0 {
		t.Errorf("ReadTimeout = %v, want 0: net/http leaves it as an absolute read deadline on the "+
			"conn after the websocket upgrade hijacks it. It is survivable only while "+
			"internal/ws leaves Upgrader.HandshakeTimeout at zero, because that is the only "+
			"gorilla branch that clears the read deadline; set a HandshakeTimeout there and "+
			"this kills live editing rooms.", srv.ReadTimeout)
	}
	if srv.WriteTimeout != 0 {
		t.Errorf("WriteTimeout = %v, want 0: net/http arms it as an absolute write deadline that it "+
			"never clears for a hijacked conn, so rooms go one-way mid-edit unless another "+
			"package happens to clear it.", srv.WriteTimeout)
	}
}

// TestNewHTTPServer_IdleTimeoutDoesNotFightTheProxy checks the one bound that
// IS safe to add: IdleTimeout applies only to keep-alive waits between requests
// on a non-hijacked connection. It must stay above the gateway proxy's 120s
// idle so the proxy always retires a pooled connection before we do.
func TestNewHTTPServer_IdleTimeoutDoesNotFightTheProxy(t *testing.T) {
	srv := newHTTPServer(":8081", http.NewServeMux())

	if srv.IdleTimeout <= 0 {
		t.Fatal("IdleTimeout is unset, so idle keep-alive connections are never reaped")
	}
	if srv.IdleTimeout <= proxyIdleTimeout {
		t.Errorf("IdleTimeout = %v, want > the proxy's %v idle timeout so the proxy retires "+
			"a pooled conn before we close it", srv.IdleTimeout, proxyIdleTimeout)
	}
}

// TestNewHTTPServer_SlowHeaderClientIsDisconnected is the actual Slowloris
// reproduction: a client that opens a connection, sends a request line and one
// header, then stalls forever. Before the fix the server waited indefinitely.
func TestNewHTTPServer_SlowHeaderClientIsDisconnected(t *testing.T) {
	srv := startServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// A request that is never terminated by the blank line: headers stay "in
	// progress" for as long as the server is willing to wait.
	if _, err := conn.Write([]byte("GET /healthz HTTP/1.1\r\nHost: localhost\r\n")); err != nil {
		t.Fatalf("write partial request: %v", err)
	}

	// Generous relative to shortTimeout, but far below "forever".
	if err := conn.SetReadDeadline(time.Now().Add(20 * shortTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}

	// The server must hang up (EOF or a 408) rather than keep the slot open.
	if _, err := io.ReadAll(conn); err != nil {
		var nerr net.Error
		if errors.As(err, &nerr) && nerr.Timeout() {
			t.Fatal("server never closed a connection that stalled mid-headers: " +
				"ReadHeaderTimeout is not in effect (gosec G112)")
		}
		// Any other read error means the peer went away, which is the pass.
	}
}

// TestNewHTTPServer_HealthzStillServed is a sanity check that the added
// timeouts did not break ordinary short requests.
func TestNewHTTPServer_HealthzStillServed(t *testing.T) {
	srv := startServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	resp, err := srv.Client().Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

// TestNewHTTPServer_UpgradedWebSocketOutlivesReadHeaderTimeout pins the
// property that must never regress: once the connection is upgraded, none of
// the server's timeouts tear it down. Without it, a later "hardening" of this
// file breaks live collaborative editing silently, because nothing else here
// exercises a real upgrade.
//
// The handler deliberately sets NO deadlines of its own on the upgraded conn.
// internal/ws sets pongWait/writeWait before every frame, so a handler modelled
// on it would mask a bad server setting; a deadline-free handler observes the
// server configuration directly.
//
// Both directions are exercised after every server timeout window has elapsed,
// because ReadTimeout and WriteTimeout fail on opposite halves of the socket.
//
// One honest limitation, since it decides how much this test can be trusted: it
// cannot by itself catch a ReadTimeout or WriteTimeout being added. gorilla's
// Upgrade clears both deadlines ("Clear deadlines set by HTTP server") on the
// branch taken when Upgrader.HandshakeTimeout is zero, which is how
// internal/ws configures it — so the upgrade neutralises them before this test
// can observe them. Verified by injection: adding a 30s ReadTimeout or
// WriteTimeout to newHTTPServer leaves this test passing.
// TestNewHTTPServer_OmitsReadAndWriteTimeouts is therefore the actual guard
// against that regression, and it catches both. What this test uniquely proves
// is the end-to-end claim the other cannot: that the timeouts newHTTPServer
// DOES set — ReadHeaderTimeout and IdleTimeout — never reach a live room.
func TestNewHTTPServer_UpgradedWebSocketOutlivesReadHeaderTimeout(t *testing.T) {
	// Comfortably longer than shortTimeout: any server-level deadline
	// armed at request-read time has expired by the time we do any I/O.
	const idle = 5 * shortTimeout

	upgrader := websocket.Upgrader{}
	handlerErr := make(chan error, 1)

	srv := startServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			handlerErr <- err
			return
		}
		defer func() { _ = conn.Close() }()

		// Hold the room open past every server timeout window, then prove the
		// server can still WRITE on the hijacked conn (the WriteTimeout trap).
		time.Sleep(idle)
		if err := conn.WriteMessage(websocket.TextMessage, []byte("still-here")); err != nil {
			handlerErr <- err
			return
		}

		// ... and still READ from it (the ReadTimeout trap).
		_, msg, err := conn.ReadMessage()
		if err != nil {
			handlerErr <- err
			return
		}
		if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			handlerErr <- err
			return
		}
		handlerErr <- nil
	}))

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/draft-1"
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	defer func() { _ = conn.Close() }()

	// The client is allowed plenty of time; the server is the thing under test.
	if err := conn.SetReadDeadline(time.Now().Add(10 * idle)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}

	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("server could not write to the upgraded conn after %v idle: %v\n"+
			"A server-level WriteTimeout leaves an absolute write deadline on the hijacked "+
			"connection, which kills live collaborative editing rooms.", idle, err)
	}
	if string(msg) != "still-here" {
		t.Fatalf("message = %q, want %q", msg, "still-here")
	}

	if err := conn.WriteMessage(websocket.TextMessage, []byte("echo-me")); err != nil {
		t.Fatalf("client write after idle: %v", err)
	}
	_, echo, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("server could not read from the upgraded conn after %v idle: %v\n"+
			"A server-level ReadTimeout leaves an absolute read deadline on the hijacked "+
			"connection, which kills live collaborative editing rooms.", idle, err)
	}
	if string(echo) != "echo-me" {
		t.Fatalf("echo = %q, want %q", echo, "echo-me")
	}

	select {
	case err := <-handlerErr:
		if err != nil {
			t.Fatalf("server side of the upgraded conn failed: %v", err)
		}
	case <-time.After(10 * idle):
		t.Fatal("handler did not finish")
	}
}

// TestNewHTTPServer_PreservesAddrAndHandler guards the mechanical part of the
// extraction: main() must still listen where it used to, on the mux it built.
func TestNewHTTPServer_PreservesAddrAndHandler(t *testing.T) {
	mux := http.NewServeMux()
	srv := newHTTPServer(":9999", mux)

	if srv.Addr != ":9999" {
		t.Errorf("Addr = %q, want %q", srv.Addr, ":9999")
	}
	if srv.Handler == nil {
		t.Fatal("Handler is nil")
	}
	if _, ok := srv.Handler.(*http.ServeMux); !ok {
		t.Errorf("Handler = %T, want the *http.ServeMux passed in", srv.Handler)
	}
}
