// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Bugs5382/go-buildinfo/health"
	grpcactor "github.com/Bugs5382/go-grpc-actor"
	log "github.com/Bugs5382/go-log"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	pbv1 "github.com/Steward-GRC/steward-collab/gen/go/steward/collab/v1"
	"github.com/Steward-GRC/steward-collab/internal/readiness"
	"github.com/Steward-GRC/steward-collab/internal/workloadauth"
)

// refusingIssuer is an OIDC issuer on a local TLS test server whose JWKS
// answers 401, as an API server does for a discovery bearer with the wrong
// audience. Its key signs tokens that look valid but can't be checked.
type refusingIssuer struct {
	url, caFile string
	key         *ecdsa.PrivateKey
}

func newRefusingIssuer(t *testing.T) *refusingIssuer {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	iss := &refusingIssuer{key: key}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"issuer": iss.url, "jwks_uri": iss.url + "/openid/v1/jwks"})
	})
	mux.HandleFunc("/openid/v1/jwks", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
	})
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	iss.url = srv.URL
	iss.caFile = filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(iss.caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600))
	return iss
}

func (i *refusingIssuer) token(t *testing.T, sa string) string {
	t.Helper()
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"iss": i.url, "aud": []string{workloadauth.DefaultAudience}, "sub": "system:serviceaccount:steward:" + sa,
		"iat": now.Unix(), "nbf": now.Unix(), "exp": now.Add(time.Hour).Unix(),
	})
	tok.Header["kid"] = "k1"
	s, err := tok.SignedString(i.key)
	require.NoError(t, err)
	return s
}

type upDB struct{}

func (upDB) Ping(context.Context) error                    { return nil }
func (upDB) ServerVersion(context.Context) (string, error) { return "16.4", nil }

type upBroker struct{}

func (upBroker) Healthy() bool { return true }

func up(context.Context) error { return nil }

// While the JWKS fetch is refused the verifier has no key, so it must never
// be inert: a valid-looking token is refused before the handler runs, and
// readiness drains the pod on both probes.
func TestWorkloadAuthFailsClosedWhileTheJWKSIsRefused(t *testing.T) {
	iss := newRefusingIssuer(t)
	v, err := workloadauth.NewVerifier(workloadauth.Config{
		Issuer: iss.url, CAFile: iss.caFile, Audience: workloadauth.DefaultAudience,
		AllowedServiceAccounts: []string{"steward/steward-gateway"},
	}, log.Nop())
	require.NoError(t, err)
	require.Error(t, v.Refresh(context.Background()), "a 401 from the JWKS is a failed refresh")

	checker, err := readiness.New(readiness.Deps{Postgres: upDB{}, Broker: upBroker{}, Identity: up, Core: up, WorkloadKeys: v.Refresh}, health.WithTTL(time.Millisecond))
	require.NoError(t, err)
	seen := make(chan grpcactor.Actor, 1)
	addr := serveAuth(t, Options{Verifier: v, Policy: testPolicy, Checker: checker, CheckInterval: 20 * time.Millisecond}, seen)

	opts, err := DialOptions(tokenFile(t, iss.token(t, "steward-gateway")))
	require.NoError(t, err)
	conn, err := grpc.NewClient(addr, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = pbv1.NewCollabRoomServiceClient(conn).FlushDraft(context.Background(), &pbv1.FlushDraftRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	select {
	case <-seen:
		t.Fatal("the handler ran without a verified caller")
	default:
	}

	hc := healthpb.NewHealthClient(conn)
	for _, svc := range []string{"", ReadinessService} {
		r, err := hc.Check(context.Background(), &healthpb.HealthCheckRequest{Service: svc})
		require.NoError(t, err)
		require.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, r.GetStatus(), "service %q", svc)
	}
	rep := checker.Report(context.Background())
	require.False(t, rep.Ready)
	var keys *health.DependencyReport
	for i := range rep.Dependencies {
		if rep.Dependencies[i].Name == readiness.WorkloadAuth {
			keys = &rep.Dependencies[i]
		}
	}
	require.NotNil(t, keys, "the key set is listed in the readiness report")
	require.True(t, keys.Required)
	require.Equal(t, health.StateDown, keys.State)

	mux := http.NewServeMux()
	require.NoError(t, RegisterProbes(mux, checker))
	probes := httptest.NewServer(mux)
	t.Cleanup(probes.Close)
	probeURL := probes.URL
	res, err := http.Get(probeURL + "/readyz")
	require.NoError(t, err)
	_ = res.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
}
