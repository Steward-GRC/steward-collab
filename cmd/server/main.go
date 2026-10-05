// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Command server runs the collab service: the websocket hub that relays live
// co-editing, the snapshot publisher that sends drafts to core, and the gRPC
// token and room services the gateway calls, in one process.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Bugs5382/go-buildinfo"
	"github.com/Bugs5382/go-buildinfo/health"
	log "github.com/Bugs5382/go-log"
	gootel "github.com/Bugs5382/go-otel"
	postgres "github.com/Bugs5382/go-postgres"
	pgotel "github.com/Bugs5382/go-postgres/otel"
	"github.com/Bugs5382/go-rabbitmq"
	rmqotel "github.com/Bugs5382/go-rabbitmq/otel"
	"google.golang.org/grpc"

	corev1 "github.com/Steward-GRC/steward-collab/gen/go/thirdparty/core/v1"
	identityv1 "github.com/Steward-GRC/steward-collab/gen/go/thirdparty/identity/v1"
	"github.com/Steward-GRC/steward-collab/internal/access"
	"github.com/Steward-GRC/steward-collab/internal/audit"
	"github.com/Steward-GRC/steward-collab/internal/config"
	"github.com/Steward-GRC/steward-collab/internal/grpcsvc"
	"github.com/Steward-GRC/steward-collab/internal/readiness"
	"github.com/Steward-GRC/steward-collab/internal/server"
	"github.com/Steward-GRC/steward-collab/internal/snapshot"
	"github.com/Steward-GRC/steward-collab/internal/store"
	"github.com/Steward-GRC/steward-collab/internal/workloadauth"
	"github.com/Steward-GRC/steward-collab/internal/ws"
)

const serviceName = "collab"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logger := log.NewLogger(serviceName)
	if err := run(ctx, logger); err != nil {
		logger.Fatal(err, "collab service stopped")
	}
}

func run(ctx context.Context, logger log.Logger) error {
	bi := buildinfo.Get()
	logger.Info("starting", log.F("version", bi.Version), log.F("commit", bi.Commit))
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	otelShutdown, err := gootel.Init(ctx, serviceName, cfg.OTLPEndpoint)
	if err != nil {
		return fmt.Errorf("otel: %w", err)
	}
	defer func() {
		if err := otelShutdown(context.Background()); err != nil {
			logger.Warn("otel shutdown", log.F("error", err.Error()))
		}
	}()

	if err := pgotel.InstrumentMigrate(ctx, serviceName, func() error {
		return postgres.Migrate(cfg.MigrateDSN, cfg.MigrationsDir)
	}); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	db, err := postgres.New(ctx, cfg.DatabaseDSN, pgotel.WithTracing())
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer db.Close()

	conn, err := rabbitmq.Connect(ctx, cfg.RabbitURL, append(rmqotel.Instrument(), rabbitmq.WithLogger(rabbitLogger{logger}))...)
	if err != nil {
		return fmt.Errorf("rabbitmq: %w", err)
	}
	defer func() { _ = conn.Close() }()
	auditPub := conn.NewPublisher(audit.Exchange,
		rabbitmq.WithExchangeDeclare(rabbitmq.ExchangeConfig{Name: audit.Exchange, Kind: "topic", Durable: true}),
		rabbitmq.WithDefaultContentType(audit.ContentType))
	auditor := audit.New(publisher{auditPub})

	dialOpts, err := server.DialOptions(cfg.TokenFile)
	if err != nil {
		return fmt.Errorf("workload token: %w", err)
	}
	coreConn, err := grpc.NewClient(cfg.CoreAddr, dialOpts...)
	if err != nil {
		return fmt.Errorf("core dial: %w", err)
	}
	defer func() { _ = coreConn.Close() }()
	identityConn, err := grpc.NewClient(cfg.IdentityAddr, dialOpts...)
	if err != nil {
		return fmt.Errorf("identity dial: %w", err)
	}
	defer func() { _ = identityConn.Close() }()

	// The room service must run over the same hub the websocket handler
	// serves, or a publish freezes no editor.
	hub := ws.NewHub(store.New(db), snapshot.NewPublisher(coreConn, auditor, logger), auditor, logger)
	go hub.Run(ctx)
	edit := access.New(identityv1.NewIdentityReadServiceClient(identityConn),
		corev1.NewPolicyServiceClient(coreConn), corev1.NewCategoryServiceClient(coreConn))
	tokens := grpcsvc.NewTokenService(grpcsvc.TokenOptions{Secret: cfg.TokenSecret, TTL: cfg.TokenTTL, WsBase: cfg.WsBase}, edit, auditor, logger)
	rooms := grpcsvc.NewRoomService(hub, auditor, logger)

	deps := readiness.Deps{
		Postgres: readiness.PostgresDB(db), Broker: conn,
		Identity: readiness.GRPCPeer(identityConn), Core: readiness.GRPCPeer(coreConn),
	}
	serveOpts := server.Options{Policy: grpcsvc.CallerPolicy, OnDeny: auditDenial(auditor, logger)}
	if cfg.WorkloadAuth {
		verifier, err := workloadauth.NewVerifier(cfg.Workload, logger)
		if err != nil {
			return fmt.Errorf("workloadauth: %w", err)
		}
		go verifier.Run(ctx)
		serveOpts.Verifier = verifier
		deps.WorkloadKeys = verifier.Refresh
	} else {
		go workloadauth.WarnDisabled(ctx, logger, workloadauth.DisabledWarnInterval)
		deps.WorkloadAuthDisabled = true
	}
	checker, err := readiness.New(deps, health.WithTTL(5*time.Second), health.WithTimeout(2*time.Second), health.WithLogger(logger))
	if err != nil {
		return fmt.Errorf("readiness: %w", err)
	}
	mux, err := httpMux(ws.NewHandler(hub, string(cfg.TokenSecret), logger).WithAllowedOrigins(cfg.AllowedOrigins), checker)
	if err != nil {
		return err
	}

	var lc net.ListenConfig
	grpcLis, err := lc.Listen(ctx, "tcp", ":"+cfg.GRPCPort)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	httpLis, err := lc.Listen(ctx, "tcp", ":"+cfg.HTTPPort)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	logger.Info("serving", log.F("grpc_port", cfg.GRPCPort), log.F("http_port", cfg.HTTPPort))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	httpDone := make(chan error, 1)
	go func() {
		httpDone <- serveHTTP(ctx, httpLis, newHTTPServer("", mux))
		cancel()
	}()
	serveOpts.Checker = checker
	err = server.Serve(ctx, grpcLis, logger, serveOpts, func(s *grpc.Server) {
		grpcsvc.RegisterTokenService(s, tokens)
		grpcsvc.RegisterRoomService(s, rooms)
	})
	cancel()
	if herr := <-httpDone; err == nil {
		err = herr
	}
	return err
}

// httpMux is the HTTP listener's routes: the websocket upgrades and the
// probes, nothing else.
func httpMux(wsHandler http.Handler, checker *health.Checker) (*http.ServeMux, error) {
	mux := http.NewServeMux()
	mux.Handle("/ws/", wsHandler)
	if err := server.RegisterProbes(mux, checker); err != nil {
		return nil, err
	}
	return mux, nil
}

// serveHTTP serves srv on lis until ctx is cancelled. Shutdown doesn't wait
// for hijacked websockets; closing the process ends them.
func serveHTTP(ctx context.Context, lis net.Listener, srv *http.Server) error {
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(lis) }()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
		if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}

// Timeouts for the websocket listener built by newHTTPServer.
const (
	// readHeaderTimeout bounds how long a client may take to send its request
	// headers. It is far below proxyIdleTimeout, so it is never the bound that
	// trips first on a healthy connection.
	readHeaderTimeout = 5 * time.Second

	// idleTimeout bounds a keep-alive connection between requests. It is
	// GREATER than proxyIdleTimeout so the gateway proxy always retires a
	// pooled connection first; were ours the shorter, the proxy could send a
	// request onto a connection we are closing.
	idleTimeout = 180 * time.Second

	// proxyIdleTimeout is the gateway proxy's idle timeout on both legs of
	// the /collab/ws/{draftID} hop, recorded so the two values above can be
	// justified against it.
	proxyIdleTimeout = 120 * time.Second
)

// newHTTPServer builds the listener for the websocket upgrades.
//
// Without a header-read bound a client could dribble headers a byte at a time
// and hold a slot forever (Slowloris, gosec G112). Which timeouts are safe
// follows from every useful connection here being a LONG-LIVED upgraded
// websocket: net/http arms its read and write deadlines on the raw net.Conn
// before the handler runs and does not clear them when the handler hijacks it.
//
//   - ReadHeaderTimeout is set. With ReadTimeout zero, net/http clears the
//     header deadline once the headers are parsed, before the handler runs, so
//     it bounds only the pre-upgrade handshake.
//
//   - ReadTimeout is NOT set. A non-zero value leaves an absolute read
//     deadline on the socket that outlives the hijack and would drop every
//     editing session that long after it opened.
//
//   - WriteTimeout is NOT set, for the same reason on the write side.
//     gorilla/websocket clears both deadlines on upgrade today, but only while
//     the upgrader's HandshakeTimeout is zero; setting these would make this
//     server's correctness depend on a field in another package. Liveness
//     already exists one layer down: internal/ws sets its own deadlines per
//     frame and pings, so a dead peer is still reaped.
//
//   - IdleTimeout is set. It applies only while waiting for the next request
//     on a keep-alive connection, which a hijacked connection never does, so
//     it can't fire on a live room. It bounds probes and refused upgrades.
func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
	}
}

// auditDenial audits every call workloadauth refuses. The request is never
// recorded, only who called what and why it was refused.
func auditDenial(auditor *audit.Emitter, logger log.Logger) workloadauth.DenyHook {
	return func(ctx context.Context, d workloadauth.Denial) {
		if err := auditor.Emit(ctx, audit.Event{
			Tier:    audit.TierAudit,
			Action:  "collab.call.refused",
			Subject: "method:" + d.Method,
			Attributes: map[string]string{
				"caller":          d.Caller.Name,
				"service_account": d.Caller.ServiceAccount,
				"code":            d.Code.String(),
				"reason":          d.Reason,
			},
		}); err != nil {
			logger.Ctx(ctx).Warn("emit collab.call.refused", log.F("error", err.Error()))
		}
	}
}

// publisher narrows a go-rabbitmq publisher to the Publish the emitter uses.
type publisher struct{ p *rabbitmq.Publisher }

func (p publisher) Publish(ctx context.Context, routingKey string, body []byte) error {
	return p.p.Publish(ctx, routingKey, body)
}

type rabbitLogger struct{ l log.Logger }

func (r rabbitLogger) Debugf(f string, a ...any) { r.l.Debug(fmt.Sprintf(f, a...)) }
func (r rabbitLogger) Infof(f string, a ...any)  { r.l.Info(fmt.Sprintf(f, a...)) }
func (r rabbitLogger) Warnf(f string, a ...any)  { r.l.Warn(fmt.Sprintf(f, a...)) }
func (r rabbitLogger) Errorf(f string, a ...any) { r.l.Error(nil, fmt.Sprintf(f, a...)) }
