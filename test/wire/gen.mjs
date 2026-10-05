// Generates REAL y-websocket / y-protocols wire frames as hex, so the Go
// collab relay can be tested against bytes produced by the actual JS packages
// (not Go-to-Go round-trips).
import * as Y from 'yjs'
import * as syncProtocol from 'y-protocols/sync.js'
import * as awarenessProtocol from 'y-protocols/awareness.js'
import * as encoding from 'lib0/encoding'
import * as decoding from 'lib0/decoding'

const messageSync = 0
const messageAwareness = 1
const messageQueryAwareness = 3

const hex = (u8) => Buffer.from(u8).toString('hex')

// --- a doc with real content -------------------------------------------------
const doc = new Y.Doc()
doc.clientID = 1193046 // 0x123456, deterministic
const text = doc.getText('root')
text.insert(0, 'hello collab')

const emptyDoc = new Y.Doc()
emptyDoc.clientID = 42

const out = {}

// versions, for the record
const { createRequire } = await import('node:module')
const req = createRequire(import.meta.url)
out.versions = {
  yjs: req('yjs/package.json').version,
  yprotocols: req('y-protocols/package.json').version,
  lib0: req('lib0/package.json').version,
}

// raw building blocks
out.stateVector = hex(Y.encodeStateVector(doc))
out.emptyStateVector = hex(Y.encodeStateVector(emptyDoc))
out.fullUpdate = hex(Y.encodeStateAsUpdate(doc))

// --- messageSync / SyncStep1 (what a real client sends on connect) ----------
{
  const e = encoding.createEncoder()
  encoding.writeVarUint(e, messageSync)
  syncProtocol.writeSyncStep1(e, doc)
  out.syncStep1 = hex(encoding.toUint8Array(e))
}
// SyncStep1 from an EMPTY doc (the "give me everything" case)
{
  const e = encoding.createEncoder()
  encoding.writeVarUint(e, messageSync)
  syncProtocol.writeSyncStep1(e, emptyDoc)
  out.syncStep1Empty = hex(encoding.toUint8Array(e))
}

// --- messageSync / SyncStep2 -------------------------------------------------
{
  const e = encoding.createEncoder()
  encoding.writeVarUint(e, messageSync)
  syncProtocol.writeSyncStep2(e, doc, Y.encodeStateVector(emptyDoc))
  out.syncStep2 = hex(encoding.toUint8Array(e))
}

// --- messageSync / Update ----------------------------------------------------
{
  const d2 = new Y.Doc()
  d2.clientID = 7
  let captured = null
  d2.on('update', (u) => { captured = u })
  d2.getText('root').insert(0, 'x')
  const e = encoding.createEncoder()
  encoding.writeVarUint(e, messageSync)
  syncProtocol.writeUpdate(e, captured)
  out.syncUpdate = hex(encoding.toUint8Array(e))
  out.syncUpdateInner = hex(captured)
}

// --- messageAwareness --------------------------------------------------------
{
  const aw = new awarenessProtocol.Awareness(doc)
  aw.setLocalStateField('user', { uid: 'spoofed-uid', name: 'Impostor', color: '#000000' })
  aw.setLocalStateField('cursor', { anchor: 1, head: 4 })
  const upd = awarenessProtocol.encodeAwarenessUpdate(aw, [doc.clientID])
  out.awarenessUpdateInner = hex(upd)
  const e = encoding.createEncoder()
  encoding.writeVarUint(e, messageAwareness)
  encoding.writeVarUint8Array(e, upd)
  out.awareness = hex(encoding.toUint8Array(e))
  aw.destroy()
}

// --- messageQueryAwareness ---------------------------------------------------
{
  const e = encoding.createEncoder()
  encoding.writeVarUint(e, messageQueryAwareness)
  out.queryAwareness = hex(encoding.toUint8Array(e))
}

console.log(JSON.stringify(out, null, 2))

process.exit(0)
