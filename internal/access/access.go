// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package access decides whether a user may co-edit a policy's draft. The
// category rules decide, through go-authz's ordered engine over the effective
// ruleset chain of the policy's home category (one core call): the user may
// edit when Author or Manage is allowed, which includes the category's owners
// and the owners of every ancestor, and the root seat. The policy's own owner,
// and a user the catalog's scoped author role (policy.author) allows in the
// policy's home category or an ancestor, keep their right as before. A
// non-root site admin edits only through a rule. The user comes from
// steward-identity and the policy and its rules from steward-core.
package access

import (
	"context"
	"errors"
	"fmt"

	"github.com/Bugs5382/go-authz/ordered"
	"github.com/Bugs5382/go-authz/rbac"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	corev1 "github.com/Steward-GRC/steward-collab/gen/go/thirdparty/core/v1"
	identityv1 "github.com/Steward-GRC/steward-collab/gen/go/thirdparty/identity/v1"
	"github.com/Steward-GRC/steward-collab/internal/authz"
)

var (
	// ErrPolicyNotFound means core has no such policy.
	ErrPolicyNotFound = errors.New("access: policy not found")
	// ErrUnavailable means core or identity couldn't answer in time.
	ErrUnavailable = errors.New("access: a peer service is unavailable")
)

// ErrAppendixNotFound means core has no such appendix.
var ErrAppendixNotFound = errors.New("access: appendix not found")

// AppendixInfo is what the appendix token needs of an appendix: the policy it
// belongs to, to run the edit check against, and whether it is retired.
type AppendixInfo struct {
	PolicyID string
	Retired  bool
}

// AppendixReader looks an appendix up in core. It returns ErrAppendixNotFound
// for an unknown id and an error wrapping ErrUnavailable when core can't answer.
type AppendixReader interface {
	GetAppendix(ctx context.Context, id string) (AppendixInfo, error)
}

// AppendixClient is the part of core's PolicyAppendixService the lookup calls.
type AppendixClient interface {
	GetPolicyAppendix(ctx context.Context, in *corev1.GetPolicyAppendixRequest, opts ...grpc.CallOption) (*corev1.GetPolicyAppendixResponse, error)
}

type appendixLookup struct{ client AppendixClient }

// NewAppendixLookup returns the AppendixReader over core.
func NewAppendixLookup(client AppendixClient) AppendixReader { return appendixLookup{client: client} }

// GetAppendix reads the appendix's policy and whether it is retired. Core
// returns a retired appendix too, with retired_at set.
func (l appendixLookup) GetAppendix(ctx context.Context, id string) (AppendixInfo, error) {
	resp, err := l.client.GetPolicyAppendix(ctx, &corev1.GetPolicyAppendixRequest{Id: id})
	if status.Code(err) == codes.NotFound {
		return AppendixInfo{}, ErrAppendixNotFound
	}
	if err != nil {
		return AppendixInfo{}, peerErr("core GetPolicyAppendix", err)
	}
	a := resp.GetAppendix()
	if a == nil {
		return AppendixInfo{}, ErrAppendixNotFound
	}
	return AppendixInfo{PolicyID: a.GetPolicyId(), Retired: a.GetRetiredAt() != nil}, nil
}

// The actions of the ordered model, as the engine spells them.
const (
	actRead    = "read"
	actAuthor  = "author"
	actApprove = "approve"
	actManage  = "manage"
	actAck     = "ack"
)

// rootRole is the engine role of the one root seat. It comes from identity's
// is_root flag, never from a name or id.
const rootRole = "root"

// model is the engine configuration: the five default actions (Manage implies
// Read and Author, Ack requires Read, owners hold Read, Author, Approve and
// Manage but not Ack) and the root seat, exempt for every action except Ack.
func model() ordered.Model {
	m := ordered.DefaultModel()
	m.Superusers = &ordered.Superusers{
		Subjects: []string{"role:" + rootRole},
		Actions:  []string{actRead, actAuthor, actApprove, actManage},
	}
	return m
}

// UserReader is the part of identity's read service the check calls.
type UserReader interface {
	GetUser(ctx context.Context, in *identityv1.GetUserRequest, opts ...grpc.CallOption) (*identityv1.GetUserResponse, error)
}

// PolicyReader is the part of core's PolicyService the check calls.
type PolicyReader interface {
	GetPolicy(ctx context.Context, in *corev1.GetPolicyRequest, opts ...grpc.CallOption) (*corev1.GetPolicyResponse, error)
}

// RulesetReader is the part of core's CategoryService the check calls: the
// whole chain of a category in one call.
type RulesetReader interface {
	GetEffectiveRuleset(ctx context.Context, in *corev1.GetEffectiveRulesetRequest, opts ...grpc.CallOption) (*corev1.GetEffectiveRulesetResponse, error)
}

// Checker makes the edit decision.
type Checker struct {
	users    UserReader
	policies PolicyReader
	rulesets RulesetReader
}

// New returns a Checker.
func New(users UserReader, policies PolicyReader, rulesets RulesetReader) *Checker {
	return &Checker{users: users, policies: policies, rulesets: rulesets}
}

// Result is the decision, and the user's display name for presence.
type Result struct {
	Allowed bool
	Name    string
}

// CanEdit decides for userID on policyID. A user identity doesn't know is
// denied, not an error.
func (c *Checker) CanEdit(ctx context.Context, userID, policyID string) (Result, error) {
	pr, err := c.policies.GetPolicy(ctx, &corev1.GetPolicyRequest{Id: policyID})
	if status.Code(err) == codes.NotFound {
		return Result{}, ErrPolicyNotFound
	}
	if err != nil {
		return Result{}, peerErr("core GetPolicy", err)
	}
	policy := pr.GetPolicy()

	ur, err := c.users.GetUser(ctx, &identityv1.GetUserRequest{UserId: userID})
	if status.Code(err) == codes.NotFound {
		return Result{}, nil
	}
	if err != nil {
		return Result{}, peerErr("identity GetUser", err)
	}
	user := ur.GetUser()
	res := Result{Name: user.GetName()}
	if policy.GetOwnerUserId() != "" && policy.GetOwnerUserId() == user.GetId() {
		res.Allowed = true
		return res, nil
	}

	levels, err := c.chain(ctx, policy.GetHomeCategoryId())
	if err != nil {
		return Result{}, err
	}
	res.Allowed = ruleAllowsEdit(levels, user) || scopedAuthor(levels, policy, user)
	return res, nil
}

// chain reads the category's rules and owners, nearest level first. A policy
// with no home category has no rules.
func (c *Checker) chain(ctx context.Context, categoryID string) ([]*corev1.EffectiveLevel, error) {
	if categoryID == "" {
		return nil, nil
	}
	resp, err := c.rulesets.GetEffectiveRuleset(ctx, &corev1.GetEffectiveRulesetRequest{CategoryId: categoryID})
	if err != nil {
		return nil, peerErr("core GetEffectiveRuleset", err)
	}
	return resp.GetLevels(), nil
}

// ruleAllowsEdit asks the ordered engine whether Author or Manage is allowed
// for the user over the chain. Owners of any level and the root seat come out
// of the engine, not from a check here.
func ruleAllowsEdit(levels []*corev1.EffectiveLevel, u *identityv1.User) bool {
	if len(levels) == 0 {
		// No chain to read: only the root seat, which the rows never bind.
		return u.GetIsRoot()
	}
	rs := ordered.RuleSet{Categories: map[string]ordered.Category{}, Parents: map[string]string{}}
	for i, l := range levels {
		owners := make([]string, 0, len(l.GetOwners()))
		for _, o := range l.GetOwners() {
			owners = append(owners, "user:"+o)
		}
		rs.Categories[l.GetCategoryId()] = ordered.Category{Entries: entries(l.GetRules()), Owners: owners}
		if i+1 < len(levels) {
			rs.Parents[l.GetCategoryId()] = levels[i+1].GetCategoryId()
		}
	}
	p := ordered.Person{ID: u.GetId(), Groups: u.GetIdpGroups()}
	if u.GetIsRoot() {
		p.Roles = []string{rootRole}
	}
	m, leaf := model(), levels[0].GetCategoryId()
	return m.Decide(rs, p, leaf, actAuthor).Allowed || m.Decide(rs, p, leaf, actManage).Allowed
}

// entries flattens the rows into the engine's cells in row order, one per
// non-blank cell.
func entries(rules []*corev1.CategoryRule) []ordered.Entry {
	var out []ordered.Entry
	for _, r := range rules {
		subject := ordered.Everyone
		switch r.GetSubjectKind() {
		case corev1.RuleSubjectKind_RULE_SUBJECT_KIND_GROUP:
			subject = "group:" + r.GetSubjectRef()
		case corev1.RuleSubjectKind_RULE_SUBJECT_KIND_USER:
			subject = "user:" + r.GetSubjectRef()
		}
		for _, cell := range []struct {
			action string
			grant  corev1.GrantEffect
		}{
			{actRead, r.GetRead()}, {actAuthor, r.GetAuthor()}, {actApprove, r.GetApprove()},
			{actManage, r.GetManage()}, {actAck, r.GetAck()},
		} {
			switch cell.grant {
			case corev1.GrantEffect_GRANT_EFFECT_ALLOW:
				out = append(out, ordered.Entry{Subject: subject, Action: cell.action, Effect: ordered.Allow})
			case corev1.GrantEffect_GRANT_EFFECT_DENY:
				out = append(out, ordered.Entry{Subject: subject, Action: cell.action, Effect: ordered.Deny})
			}
		}
	}
	return out
}

// scopedAuthor keeps the right the catalog's scoped author role gives: a
// policy.author grant scoped to the policy's home category or an ancestor.
// The catalog's site-admin bypass is left out of the subject, so an admin
// edits through a rule alone, and root is the engine's seat.
func scopedAuthor(levels []*corev1.EffectiveLevel, policy *corev1.Policy, u *identityv1.User) bool {
	lineage := make([]string, 0, len(levels))
	for _, l := range levels {
		lineage = append(lineage, l.GetName())
	}
	resource := &rbac.Resource{ID: policy.GetNumber(), CategoryLineage: lineage}
	if len(lineage) > 0 {
		resource.Category = lineage[0]
	}
	return authz.Catalog.Authorize(subject(u), authz.PolicyAuthor, resource).Allowed()
}

// subject turns an identity user into an rbac subject. Names the catalog
// doesn't know are left out, and so is the site-admin role (see scopedAuthor).
func subject(u *identityv1.User) rbac.Subject {
	s := rbac.Subject{UserID: u.GetId(), Groups: u.GetIdpGroups()}
	for _, name := range u.GetRoles() {
		r, err := authz.Catalog.ParseRole(name)
		if err != nil || r == authz.RoleSiteAdmin {
			continue
		}
		s.Roles = append(s.Roles, r)
	}
	for _, sr := range u.GetScopedRoles() {
		if r, err := authz.Catalog.ParseRole(sr.GetRole()); err == nil {
			s.ScopedGrants = append(s.ScopedGrants, rbac.ScopedGrant{Role: r, Category: sr.GetCategory()})
		}
	}
	return s
}

func peerErr(call string, err error) error {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return fmt.Errorf("%s: %w: %w", call, ErrUnavailable, err)
	}
	return fmt.Errorf("%s: %w", call, err)
}
