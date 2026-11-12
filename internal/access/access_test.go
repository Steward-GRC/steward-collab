// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package access_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	corev1 "github.com/Steward-GRC/steward-collab/gen/go/thirdparty/core/v1"
	identityv1 "github.com/Steward-GRC/steward-collab/gen/go/thirdparty/identity/v1"
	"github.com/Steward-GRC/steward-collab/internal/access"
	"github.com/Steward-GRC/steward-collab/internal/fixture"
)

const policyID = "policy-desk-booking"

type fakeUsers struct {
	users map[string]*identityv1.User
	err   error
}

func (f fakeUsers) GetUser(_ context.Context, in *identityv1.GetUserRequest, _ ...grpc.CallOption) (*identityv1.GetUserResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	u, ok := f.users[in.GetUserId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such user")
	}
	return &identityv1.GetUserResponse{User: u}, nil
}

type fakePolicies struct {
	policy *corev1.Policy
	err    error
}

func (f fakePolicies) GetPolicy(_ context.Context, in *corev1.GetPolicyRequest, _ ...grpc.CallOption) (*corev1.GetPolicyResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.policy == nil || in.GetId() != f.policy.GetId() {
		return nil, status.Error(codes.NotFound, "no such policy")
	}
	return &corev1.GetPolicyResponse{Policy: f.policy}, nil
}

// fakeRulesets answers GetEffectiveRuleset from per-category chains, nearest
// level first, as core does.
type fakeRulesets struct {
	chains map[string][]*corev1.EffectiveLevel
	err    error
	asked  []string
}

func (f *fakeRulesets) GetEffectiveRuleset(_ context.Context, in *corev1.GetEffectiveRulesetRequest, _ ...grpc.CallOption) (*corev1.GetEffectiveRulesetResponse, error) {
	f.asked = append(f.asked, in.GetCategoryId())
	if f.err != nil {
		return nil, f.err
	}
	levels, ok := f.chains[in.GetCategoryId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such category")
	}
	return &corev1.GetEffectiveRulesetResponse{Levels: levels}, nil
}

const (
	allow = corev1.GrantEffect_GRANT_EFFECT_ALLOW
	deny  = corev1.GrantEffect_GRANT_EFFECT_DENY
)

// cells are a row's grants, blank when left out.
type cells struct{ read, author, manage corev1.GrantEffect }

func rule(kind corev1.RuleSubjectKind, ref string, c cells) *corev1.CategoryRule {
	return &corev1.CategoryRule{SubjectKind: kind, SubjectRef: ref, Read: c.read, Author: c.author, Manage: c.manage}
}

func userRule(id string, c cells) *corev1.CategoryRule {
	return rule(corev1.RuleSubjectKind_RULE_SUBJECT_KIND_USER, id, c)
}

func groupRule(name string, c cells) *corev1.CategoryRule {
	return rule(corev1.RuleSubjectKind_RULE_SUBJECT_KIND_GROUP, name, c)
}

func everyoneRule(c cells) *corev1.CategoryRule {
	return rule(corev1.RuleSubjectKind_RULE_SUBJECT_KIND_EVERYONE, "", c)
}

// Extra people, local to this file.
const (
	manager   = "user-manager"    // Manage cell on Facilities
	rowAuthor = "user-row-author" // Author cell on Facilities
	grouped   = "user-grouped"    // in a group the Facilities rows allow to author
	denied    = "user-denied"     // denied Author ahead of a group allow
	parentOwn = "user-parent-own" // owns Workplace, the parent
	reader    = "user-reader"     // may read only
	siteAdmin = "user-site-admin" // site-admin role, not root
	adminRule = "user-admin-rule" // site-admin role with an Author cell
	rootUser  = "user-root"       // the root seat
	stranger  = "user-stranger"   // no rule, no role
)

// The brief's tree: Workplace with Facilities below it, Finance apart.
func rulesets() *fakeRulesets {
	facilities := &corev1.EffectiveLevel{CategoryId: fixture.Facilities, Name: "Facilities", Rules: []*corev1.CategoryRule{
		userRule(denied, cells{author: deny}),
		userRule(rowAuthor, cells{author: allow}),
		userRule(manager, cells{manage: allow}),
		userRule(adminRule, cells{author: allow}),
		groupRule(fixture.FacilitiesTeam, cells{author: allow}),
		everyoneRule(cells{read: allow}),
	}}
	workplace := &corev1.EffectiveLevel{CategoryId: fixture.Workplace, Name: "Workplace", Owners: []string{parentOwn}}
	finance := &corev1.EffectiveLevel{CategoryId: fixture.Finance, Name: "Finance"}
	f := &fakeRulesets{chains: map[string][]*corev1.EffectiveLevel{
		fixture.Facilities: {facilities, workplace},
		fixture.Workplace:  {workplace},
		fixture.Finance:    {finance},
	}}
	return f
}

var deskBooking = &corev1.Policy{
	Id: policyID, Number: fixture.DeskBookingPolicyNumber, Title: fixture.DeskBookingPolicy,
	HomeCategoryId: fixture.Facilities, OwnerUserId: fixture.Dave,
}

func users() fakeUsers {
	return fakeUsers{users: map[string]*identityv1.User{
		fixture.Alice: {Id: fixture.Alice, Name: "Alice", Roles: []string{"site-admin"}, IsRoot: true},
		fixture.Bob: {Id: fixture.Bob, Name: "Bob", Roles: []string{"author"},
			ScopedRoles: []*identityv1.ScopedRole{{Role: "author", Category: "Facilities"}}},
		fixture.Carol: {Id: fixture.Carol, Name: "Carol",
			ScopedRoles: []*identityv1.ScopedRole{{Role: "approver", Category: "Facilities"}}},
		fixture.Dave: {Id: fixture.Dave, Name: "Dave"},
		fixture.Erin: {Id: fixture.Erin, Name: "Erin",
			ScopedRoles: []*identityv1.ScopedRole{{Role: "author", Category: "Workplace"}}},
		fixture.Frank: {Id: fixture.Frank, Name: "Frank", Roles: []string{"author"},
			ScopedRoles: []*identityv1.ScopedRole{{Role: "author", Category: "Finance"}}},
		fixture.Grace: {Id: fixture.Grace, Name: "Grace", Roles: []string{"author"}},
		manager:       {Id: manager, Name: "Manager"},
		rowAuthor:     {Id: rowAuthor, Name: "Row Author"},
		grouped:       {Id: grouped, Name: "Grouped", IdpGroups: []string{fixture.FacilitiesTeam}},
		// The group row would allow Author, but the user's own row above it denies.
		denied:    {Id: denied, Name: "Denied", IdpGroups: []string{fixture.FacilitiesTeam}},
		parentOwn: {Id: parentOwn, Name: "Parent Owner"},
		reader:    {Id: reader, Name: "Reader"},
		siteAdmin: {Id: siteAdmin, Name: "Site Admin", Roles: []string{"site-admin"}},
		adminRule: {Id: adminRule, Name: "Admin With Rule", Roles: []string{"site-admin"}},
		rootUser:  {Id: rootUser, Name: "Root", Roles: []string{"site-admin"}, IsRoot: true},
		stranger:  {Id: stranger, Name: "Stranger"},
	}}
}

func checker(u fakeUsers, p fakePolicies) *access.Checker {
	return access.New(u, p, rulesets())
}

func TestCanEdit(t *testing.T) {
	cases := []struct {
		name    string
		user    string
		allowed bool
	}{
		// Rights the check already gave and must keep (D6).
		{"an author scoped to the home category", fixture.Bob, true},
		{"an author scoped to an ancestor", fixture.Erin, true},
		{"the policy owner", fixture.Dave, true},
		{"an approver is not an author", fixture.Carol, false},
		{"an author scoped elsewhere", fixture.Frank, false},
		{"a global author role without a scoped grant", fixture.Grace, false},
		// The category rules.
		{"a user row with an Author cell", rowAuthor, true},
		{"a user row with a Manage cell", manager, true},
		{"a member of a group row with an Author cell", grouped, true},
		{"an inherited category owner", parentOwn, true},
		{"root, with no row", rootUser, true},
		{"a reader", reader, false},
		{"a user with no row and no role", stranger, false},
		{"a non-root site admin without a rule", siteAdmin, false},
		{"a non-root site admin with an Author cell", adminRule, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := checker(users(), fakePolicies{policy: deskBooking}).CanEdit(context.Background(), c.user, policyID)
			require.NoError(t, err)
			require.Equal(t, c.allowed, got.Allowed)
		})
	}
}

// The first row that answers an action decides it: the denied user is in the
// group the allow row names, but their own deny row sits above it.
func TestCanEditAuthorDenyBlocksANonOwner(t *testing.T) {
	got, err := checker(users(), fakePolicies{policy: deskBooking}).CanEdit(context.Background(), denied, policyID)
	require.NoError(t, err)
	require.False(t, got.Allowed)
}

// A deny never takes the policy owner's or a category owner's right.
func TestCanEditOwnersAreImmuneToDeny(t *testing.T) {
	rs := rulesets()
	rs.chains[fixture.Facilities][0].Rules = append([]*corev1.CategoryRule{
		userRule(parentOwn, cells{author: deny, manage: deny}),
	}, rs.chains[fixture.Facilities][0].Rules...)
	got, err := access.New(users(), fakePolicies{policy: deskBooking}, rs).CanEdit(context.Background(), parentOwn, policyID)
	require.NoError(t, err)
	require.True(t, got.Allowed)
}

func TestCanEditAsksCoreOnceForTheHomeCategoryChain(t *testing.T) {
	rs := rulesets()
	_, err := access.New(users(), fakePolicies{policy: deskBooking}, rs).CanEdit(context.Background(), rowAuthor, policyID)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.Facilities}, rs.asked)
}

func TestCanEditReportsAnUnreachableCore(t *testing.T) {
	rs := &fakeRulesets{err: status.Error(codes.Unavailable, "connection refused")}
	_, err := access.New(users(), fakePolicies{policy: deskBooking}, rs).CanEdit(context.Background(), rowAuthor, policyID)
	require.ErrorIs(t, err, access.ErrUnavailable)
}

func TestCanEditAPolicyWithNoHomeCategoryFollowsNoRules(t *testing.T) {
	p := &corev1.Policy{Id: policyID, OwnerUserId: fixture.Dave}
	c := checker(users(), fakePolicies{policy: p})
	got, err := c.CanEdit(context.Background(), rowAuthor, policyID)
	require.NoError(t, err)
	require.False(t, got.Allowed)
	got, err = c.CanEdit(context.Background(), rootUser, policyID)
	require.NoError(t, err)
	require.True(t, got.Allowed)
}

func TestCanEditReturnsTheDisplayName(t *testing.T) {
	got, err := checker(users(), fakePolicies{policy: deskBooking}).CanEdit(context.Background(), fixture.Bob, policyID)
	require.NoError(t, err)
	require.Equal(t, "Bob", got.Name)
}

func TestCanEditUnknownPolicy(t *testing.T) {
	_, err := checker(users(), fakePolicies{policy: deskBooking}).CanEdit(context.Background(), fixture.Bob, "policy-missing")
	require.ErrorIs(t, err, access.ErrPolicyNotFound)
}

func TestCanEditUnknownUserIsDenied(t *testing.T) {
	got, err := checker(users(), fakePolicies{policy: deskBooking}).CanEdit(context.Background(), "user-nobody", policyID)
	require.NoError(t, err)
	require.False(t, got.Allowed)
}

func TestCanEditReportsAnUnreachablePeer(t *testing.T) {
	down := status.Error(codes.Unavailable, "connection refused")
	_, err := checker(users(), fakePolicies{err: down}).CanEdit(context.Background(), fixture.Bob, policyID)
	require.ErrorIs(t, err, access.ErrUnavailable)

	_, err = checker(fakeUsers{err: down}, fakePolicies{policy: deskBooking}).CanEdit(context.Background(), fixture.Bob, policyID)
	require.ErrorIs(t, err, access.ErrUnavailable)
}

func TestCanEditPassesOtherFailuresThrough(t *testing.T) {
	boom := errors.New("boom")
	_, err := checker(users(), fakePolicies{err: boom}).CanEdit(context.Background(), fixture.Bob, policyID)
	require.ErrorIs(t, err, boom)
	require.NotErrorIs(t, err, access.ErrUnavailable)
}
