// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package server runs collab's gRPC server and its HTTP probes.
package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/Bugs5382/go-buildinfo/grpcbuildinfo"
	"github.com/Bugs5382/go-buildinfo/health"
	"github.com/Bugs5382/go-buildinfo/httpbuildinfo"
	grpcactor "github.com/Bugs5382/go-grpc-actor"
	log "github.com/Bugs5382/go-log"
	gootel "github.com/Bugs5382/go-otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpchealth "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/Steward-GRC/steward-collab/internal/workloadauth"
)

// gracefulStopTimeout bounds the drain of in-flight RPCs on shutdown, so a
// hung peer can't hold the process.
const gracefulStopTimeout = 10 * time.Second

// HealthPrefix starts the build and dependency headers: steward-version,
// steward-commit, steward-dep-<name> and steward-depstate-<name>.
const HealthPrefix = "steward"

// grpc.health.v1 service names. The empty name and ReadinessService follow
// readiness; LivenessService reports the process only, so a dependency outage
// never gets collab restarted.
const (
	ReadinessService = "readiness"
	LivenessService  = "liveness"
)

// Options are the authentication and probe settings. A nil Verifier is
// WORKLOAD_AUTH=disabled: every caller is let in and its actor believed, for
// local runs only.
type Options struct {
	// Verifier checks the caller's projected service-account token.
	Verifier workloadauth.TokenVerifier
	// Policy is the per-method caller allow-list.
	Policy workloadauth.Policy
	// OnDeny is called for every refused call, to audit it.
	OnDeny workloadauth.DenyHook
	// Checker holds the dependencies readiness follows.
	Checker *health.Checker
	// CheckInterval is how often Health/Watch subscribers are brought up to
	// date (go-buildinfo's default when zero).
	CheckInterval time.Duration
}

// Serve runs a gRPC server on lis with go-otel tracing, panic recovery,
// workloadauth, go-grpc-actor, grpc.health.v1 and reflection, plus the
// services register adds. Health needs no token. It returns nil once ctx is
// cancelled and the server has stopped.
func Serve(ctx context.Context, lis net.Listener, lg log.Logger, opts Options, register func(*grpc.Server)) error {
	checker := opts.Checker
	if checker == nil {
		checker = health.New()
	}
	hs := grpchealth.NewServer()
	biOpts := []grpcbuildinfo.Option{
		grpcbuildinfo.WithPrefix(HealthPrefix), grpcbuildinfo.WithChecker(checker), grpcbuildinfo.WithHealthServer(hs),
		grpcbuildinfo.WithServices(ReadinessService), grpcbuildinfo.WithLivenessService(LivenessService),
	}
	if opts.CheckInterval > 0 {
		biOpts = append(biOpts, grpcbuildinfo.WithInterval(opts.CheckInterval))
	}
	bi, err := grpcbuildinfo.New(biOpts...)
	if err != nil {
		return fmt.Errorf("server: health: %w", err)
	}
	unary := []grpc.UnaryServerInterceptor{bi.UnaryServerInterceptor(), recoverUnary(lg)}
	stream := []grpc.StreamServerInterceptor{bi.StreamServerInterceptor(), recoverStream(lg)}
	trust := grpcactor.TrustFunc(func(context.Context, string) bool { return true })
	if opts.Verifier != nil {
		var authOpts []workloadauth.Option
		if opts.OnDeny != nil {
			authOpts = append(authOpts, workloadauth.WithDenyHook(opts.OnDeny))
		}
		unary = append(unary, workloadauth.UnaryServerInterceptor(opts.Verifier, opts.Policy, lg, authOpts...))
		stream = append(stream, workloadauth.StreamServerInterceptor(opts.Verifier, opts.Policy, lg, authOpts...))
		trust = actorPassing
	}
	unary = append(unary, grpcactor.UnaryServerInterceptor(grpcactor.WithTrust(trust)))
	stream = append(stream, grpcactor.StreamServerInterceptor(grpcactor.WithTrust(trust)))
	s := grpc.NewServer(
		grpc.StatsHandler(gootel.GRPCServerStatsHandler()),
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(stream...),
	)
	healthpb.RegisterHealthServer(s, hs)
	bi.Update(ctx)
	go bi.Run(ctx)
	reflection.Register(s)
	register(s)

	errCh := make(chan error, 1)
	go func() { errCh <- s.Serve(lis) }()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		hs.Shutdown()
		stopped := make(chan struct{})
		go func() {
			s.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(gracefulStopTimeout):
			s.Stop()
		}
		<-errCh
		return nil
	}
}

// actorPassing believes an end-user actor only from a caller the policy lists
// as acting on a user's behalf for this method; any other caller acts as
// itself.
func actorPassing(ctx context.Context, _ string) bool {
	g, ok := workloadauth.GrantFromContext(ctx)
	return ok && g.Access == workloadauth.OnBehalf
}

// RegisterProbes adds /livez and /readyz to mux, the HTTP listener the
// websocket upgrades already use, for probes that can't speak gRPC. Liveness
// is the process only; readiness follows checker. There is no plain /health.
func RegisterProbes(mux *http.ServeMux, checker *health.Checker) error {
	h, err := httpbuildinfo.New(httpbuildinfo.WithPrefix(HealthPrefix), httpbuildinfo.WithChecker(checker))
	if err != nil {
		return fmt.Errorf("server: probes: %w", err)
	}
	mux.Handle("GET /livez", h.Livez())
	mux.Handle("GET /readyz", h.Readyz())
	return nil
}

// The panic value and stack go to the log only; the caller gets a bare
// Internal.
func recoverUnary(lg log.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				resp, err = nil, recovered(ctx, lg, r, info.FullMethod)
			}
		}()
		return handler(ctx, req)
	}
}

func recoverStream(lg log.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = recovered(ss.Context(), lg, r, info.FullMethod)
			}
		}()
		return handler(srv, ss)
	}
}

func recovered(ctx context.Context, lg log.Logger, r any, method string) error {
	lg.Ctx(ctx).Error(nil, "recovered from a panic in a gRPC handler",
		log.F("method", method), log.F("panic", r), log.F("stack", string(debug.Stack())))
	return status.Error(codes.Internal, "internal error")
}
