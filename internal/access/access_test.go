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

// The brief's tree: Workplace with Facilities below it.
type fakeCategories map[string]*corev1.Category

func (f fakeCategories) GetCategory(_ context.Context, in *corev1.GetCategoryRequest, _ ...grpc.CallOption) (*corev1.GetCategoryResponse, error) {
	c, ok := f[in.GetId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such category")
	}
	return &corev1.GetCategoryResponse{Category: c}, nil
}

var categories = fakeCategories{
	fixture.Workplace:  {Id: fixture.Workplace, Name: "Workplace"},
	fixture.Facilities: {Id: fixture.Facilities, Name: "Facilities", ParentId: fixture.Workplace},
	fixture.Finance:    {Id: fixture.Finance, Name: "Finance"},
}

var deskBooking = &corev1.Policy{
	Id: policyID, Number: fixture.DeskBookingPolicyNumber, Title: fixture.DeskBookingPolicy,
	HomeCategoryId: fixture.Facilities, OwnerUserId: fixture.Dave,
}

func users() fakeUsers {
	return fakeUsers{users: map[string]*identityv1.User{
		fixture.Alice: {Id: fixture.Alice, Name: "Alice", Roles: []string{"site-admin"}},
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
	}}
}

func checker(u fakeUsers, p fakePolicies) *access.Checker {
	return access.New(u, p, categories)
}

func TestCanEdit(t *testing.T) {
	cases := []struct {
		name    string
		user    string
		allowed bool
	}{
		{"an author scoped to the home category", fixture.Bob, true},
		{"an author scoped to an ancestor", fixture.Erin, true},
		{"the owner", fixture.Dave, true},
		{"a site admin", fixture.Alice, true},
		{"an approver is not an author", fixture.Carol, false},
		{"an author scoped elsewhere", fixture.Frank, false},
		{"a global author role without a scoped grant", fixture.Grace, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := checker(users(), fakePolicies{policy: deskBooking}).CanEdit(context.Background(), c.user, policyID)
			require.NoError(t, err)
			require.Equal(t, c.allowed, got.Allowed)
		})
	}
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
