# y-protocols wire fixtures

The relay's framing is only correct if it matches what the **real** Yjs
JavaScript stack writes and reads. A Go-to-Go round-trip test cannot prove
that: a self-consistent encoder/decoder pair passes it while being unreadable
to every browser client.

These two Node scripts are how the fixtures in
`internal/ws/protocol_yprotocols_test.go` were produced and verified. They are
**not** part of CI (the Go test carries the resulting bytes as constants, so it
needs no Node toolchain); run them by hand when the protocol changes or when a
y-protocols upgrade needs re-verifying.

## Setup

The fixtures were made with these versions:

```sh
cd test/wire
npm install --no-save yjs@13.6.32 y-protocols@1.0.7 lib0@0.2.117
```

## Generate fixtures (JS → hex)

Prints every frame a stock client sends, as hex, ready to paste into the Go
test constants:

```sh
node gen.mjs
```

## Verify our frames (hex → JS)

Feeds a hex frame into the real `y-protocols` readers exactly the way
y-websocket's message handlers do, and reports whether Yjs accepted it. Use it
on bytes captured from the Go encoder:

```sh
# a correct sync step 1
node consume.mjs 00000501d6e8480c
# {"ok":true,"messageType":0,"syncType":0,"reply":"…","docText":"seed"}
```

`{"ok":false,"error":"Unexpected end of array"}` means the frame is malformed —
that is what a missing `varUint8Array` length prefix looks like from the client
side. Watch stderr too: for **update** frames Yjs catches the decode failure
internally ("Caught error while handling a Yjs update") and returns `ok:true`
while silently discarding the edit, so `ok:true` alone is not proof — check
`docText` actually changed.
