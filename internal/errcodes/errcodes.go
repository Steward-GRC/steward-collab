// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package errcodes holds the collab service's coded errors (band 8, from 8500;
// delivery holds 8000 to 8499) and turns them into gRPC statuses through
// go-apperr.
package errcodes

import (
	"context"
	"sync"

	apperr "github.com/Bugs5382/go-apperr"
	"github.com/Bugs5382/go-apperr/apperrgrpc"
	log "github.com/Bugs5382/go-log"
)

// Domain is the ErrorInfo domain every collab error carries.
const Domain = "collab"

// The collab service's codes.
const (
	CodeInternal             = 8500
	CodePolicyIDRequired     = 8501
	CodeDraftIDRequired      = 8502
	CodeVersionNumberInvalid = 8503
	CodeActorRequired        = 8504
	CodeDraftEditForbidden   = 8505
	CodeRoomPolicyMismatch   = 8506
	CodeFlushRejected        = 8507
	CodeFlushUnavailable     = 8508
	CodeFlushTimeout         = 8509
	CodePolicyNotFound       = 8510
	CodeEditCheckUnavailable = 8511
)

// Entries returns the registry entries.
func Entries() []apperr.Entry {
	return []apperr.Entry{
		{Code: CodeInternal, Symbol: "INTERNAL", Category: apperr.CategoryInternal,
			Title: "collab", Cause: "an uncoded failure inside the collab service"},
		{Code: CodePolicyIDRequired, Symbol: "POLICY_ID_REQUIRED", Category: apperr.CategoryInvalid,
			Title: "request", Cause: "the request has no policy_id; the gateway always sets it, so this is a client bug"},
		{Code: CodeDraftIDRequired, Symbol: "DRAFT_ID_REQUIRED", Category: apperr.CategoryInvalid,
			Title: "request", Cause: "the request has no draft_id; the gateway always sets it, so this is a client bug"},
		{Code: CodeVersionNumberInvalid, Symbol: "VERSION_NUMBER_INVALID", Category: apperr.CategoryInvalid,
			Title: "publish", Cause: "the published version number is not positive, so nothing was published (0 is core's draft sentinel)"},
		{Code: CodeActorRequired, Symbol: "ACTOR_REQUIRED", Category: apperr.CategoryUnauthenticated,
			Title: "request", Cause: "the call carries no go-grpc-actor actor, so there is nobody to mint a token for or attribute the action to"},
		{Code: CodeDraftEditForbidden, Symbol: "DRAFT_EDIT_FORBIDDEN", Category: apperr.CategoryPermissionDenied,
			Title: "token", Cause: "the caller neither owns the policy nor holds an author grant in its home category or an ancestor",
			UserSafe: true, Message: "You don't have permission to edit this draft."},
		{Code: CodeRoomPolicyMismatch, Symbol: "ROOM_POLICY_MISMATCH", Category: apperr.CategoryFailedPrecondition,
			Title: "room", Cause: "the live room for the draft belongs to a different policy than the request names, so the room is left alone"},
		{Code: CodeFlushRejected, Symbol: "FLUSH_REJECTED", Category: apperr.CategoryFailedPrecondition,
			Title: "flush", Cause: "core refused the live room's newest checkpoint (reason and detail in the metadata), so core doesn't hold the room's content",
			UserSafe: true, Message: "The latest edits couldn't be saved, so the draft wasn't published: {detail}"},
		{Code: CodeFlushUnavailable, Symbol: "FLUSH_UNAVAILABLE", Category: apperr.CategoryUnavailable,
			Title: "flush", Cause: "core couldn't be reached to save the live room's newest checkpoint; it stays queued for a retry",
			UserSafe: true, Message: "The latest edits couldn't be saved yet, so the draft wasn't published. Try again in a moment."},
		{Code: CodeFlushTimeout, Symbol: "FLUSH_TIMEOUT", Category: apperr.CategoryDeadlineExceeded,
			Title: "flush", Cause: "core didn't answer the live room's checkpoint in time; it stays queued for a retry",
			UserSafe: true, Message: "The latest edits couldn't be saved yet, so the draft wasn't published. Try again in a moment."},
		{Code: CodePolicyNotFound, Symbol: "POLICY_NOT_FOUND", Category: apperr.CategoryNotFound,
			Title: "token", Cause: "core has no policy with the requested id",
			UserSafe: true, Message: "This policy no longer exists."},
		{Code: CodeEditCheckUnavailable, Symbol: "EDIT_CHECK_UNAVAILABLE", Category: apperr.CategoryUnavailable,
			Title: "token", Cause: "core or identity couldn't be reached to decide whether the caller may edit the draft",
			UserSafe: true, Message: "Co-editing can't start right now. Try again in a moment."},
	}
}

var (
	regOnce sync.Once
	reg     *apperr.Registry
)

// Registry returns the service registry. Coded errors are logged through
// go-log with the trace of the request they failed.
func Registry() *apperr.Registry {
	regOnce.Do(func() {
		r, err := apperr.NewRegistry(Entries(), apperr.WithService(8), apperr.WithCodeDigits(4),
			apperr.WithLogger(logSink{log.NewLogger("collab")}))
		if err != nil {
			panic(err)
		}
		reg = r
	})
	return reg
}

// Error turns err into the gRPC error a handler returns.
func Error(ctx context.Context, err error) error {
	return apperrgrpc.Error(ctx, Registry(), err, CodeInternal, Domain)
}

// New returns the gRPC error for code with the given metadata pairs, in
// key, value order.
func New(ctx context.Context, code int, kv ...string) error {
	pairs := make([]apperr.MetaPair, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		pairs = append(pairs, apperr.Meta(kv[i], kv[i+1]))
	}
	entry, _ := Registry().Describe(code)
	return Error(ctx, apperr.WithMeta(apperr.Coded(code, refusal(entry.Symbol)), pairs...))
}

// Doc is the Markdown body of docs/error-codes.md.
func Doc() string {
	return "# Error codes\n\nEvery coded gRPC error from the collab service carries an `ErrorInfo` with the symbol as\n" +
		"its reason, the domain `" + Domain + "` and the code in `codeNum`. Only user-safe messages reach\n" +
		"the caller; every other code is sent as `Code N: Internal Error`.\n\n" + Registry().Markdown()
}

type refusal string

func (r refusal) Error() string { return "collab: " + string(r) }

type logSink struct{ l log.Logger }

func (s logSink) LogCoded(ctx context.Context, code int, err error) {
	s.l.Ctx(ctx).Debug("coded error", log.F("code", code), log.F("error", err.Error()))
}
