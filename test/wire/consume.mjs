// Feeds a hex-encoded frame into the REAL y-protocols readers, exactly the way
// y-websocket's messageHandlers do, and reports what happens.
// usage: node consume.mjs <hex>
import * as Y from 'yjs'
import * as syncProtocol from 'y-protocols/sync.js'
import * as awarenessProtocol from 'y-protocols/awareness.js'
import * as encoding from 'lib0/encoding'
import * as decoding from 'lib0/decoding'

const messageSync = 0
const messageAwareness = 1

const buf = new Uint8Array(Buffer.from(process.argv[2], 'hex'))
const doc = new Y.Doc()
doc.clientID = 99
doc.getText('root').insert(0, 'seed')
const aw = new awarenessProtocol.Awareness(doc)

const dec = decoding.createDecoder(buf)
const enc = encoding.createEncoder()
try {
  const messageType = decoding.readVarUint(dec)
  if (messageType === messageSync) {
    encoding.writeVarUint(enc, messageSync)
    const syncType = syncProtocol.readSyncMessage(dec, enc, doc, 'consume.mjs')
    console.log(JSON.stringify({
      ok: true, messageType, syncType,
      reply: Buffer.from(encoding.toUint8Array(enc)).toString('hex'),
      docText: doc.getText('root').toString(),
    }))
  } else if (messageType === messageAwareness) {
    awarenessProtocol.applyAwarenessUpdate(aw, decoding.readVarUint8Array(dec), 'consume.mjs')
    console.log(JSON.stringify({
      ok: true, messageType,
      states: Array.from(aw.getStates().entries()),
    }))
  } else {
    console.log(JSON.stringify({ ok: true, messageType, note: 'no payload read' }))
  }
} catch (err) {
  console.log(JSON.stringify({ ok: false, error: String(err && err.message || err) }))
}
aw.destroy()
process.exit(0)
