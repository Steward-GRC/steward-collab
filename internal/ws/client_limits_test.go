// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package ws

import "testing"

// The gateway's collab websocket proxy caps the frames it relays both ways, and
// that cap must stay strictly above maxMessageSize: browser to collab, so
// collab's limit is what refuses an oversized frame; collab to browser, because
// a full Yjs state can exceed even this limit. The gateway can't import the
// unexported constant, so it mirrors it as collabws.CollabReadLimitBytes. If
// this test fails, change the gateway's mirror in the same change, or the
// proxy becomes the tighter of the two and closes large snapshots with 1009.
func TestMaxMessageSize_MatchesGatewayProxyMirror(t *testing.T) {
	// steward-gateway internal/collabws: CollabReadLimitBytes = 2 << 20.
	const gatewayMirroredValue = 2 << 20 // 2 MiB

	if maxMessageSize != gatewayMirroredValue {
		t.Fatalf(
			"maxMessageSize (%d) no longer matches the value the gateway's collab WS "+
				"proxy mirrors as collabws.CollabReadLimitBytes (%d).\n"+
				"Raising this limit alone makes the gateway proxy the tighter of the two "+
				"and it will close large snapshots with 1009.\n"+
				"Update collabws.CollabReadLimitBytes in the gateway in the same change.",
			maxMessageSize, gatewayMirroredValue,
		)
	}
}
