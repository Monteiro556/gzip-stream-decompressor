# gzip-stream-decompressor

Decompresses gzip data using Node's `zlib` streams with error handling that
separates corruption from truncation.

```js
import { gzipSync } from 'node:zlib';
import { GzipStreamDecompressor } from 'gzip-stream-decompressor';

const dec = new GzipStreamDecompressor();

const original = Buffer.from('hello world', 'utf8');
const gzippedBuffer = gzipSync(original);

// Whole-buffer API: resolves to a Buffer, rejects on corruption or truncation.
const plain = await dec.decompressBuffer(gzippedBuffer);

// Streaming API: a Transform you write into and read from.
const stream = dec.decompressStream();
stream.end(gzippedBuffer);
```

## Why this exists

Node's `zlib.gunzipSync` throws the same `ZlibError` for both a corrupted
checksum and a truncated file. When a downstream system needs to distinguish
those two cases — e.g. retry on truncation, alert on corruption — `gunzipSync`
doesn't give enough signal. This library wraps `zlib.createGunzip` so that a
stream that closes without emitting `error` or `end` is treated as truncation,
and an emitted `error` is treated as corruption.

Multi-member gzip files (concatenated gzip streams) are supported because the
underlying `createGunzip` transform reads until its input is exhausted. No
special handling is needed.

## Edge you'll hit

Node's zlib does not surface the distinction between truncation and corruption
cleanly. The logic here uses the stream's `error` event as a signal: if the
stream errors, it's corruption; if it closes cleanly but produced no output,
it's truncation. A truncated file that happens to cut off after a valid
deflate block may still be reported as corruption rather than truncation
because the stream itself throws on the unexpected EOF. This is the best
Node's public stream API offers without re-implementating inflate.
