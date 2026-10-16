import { test } from 'node:test';
import assert from 'node:assert/strict';
import { gzipSync } from 'node:zlib';
import { GzipStreamDecompressor } from '../src/index.js';

/**
 * Consume all chunks from a readable stream into a single Buffer.
 * @param {import('node:stream').Readable} stream
 * @returns {Promise<Buffer>}
 */
function collectStream(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on('data', (c) => chunks.push(c));
    stream.on('end', () => resolve(Buffer.concat(chunks)));
    stream.on('error', (err) => reject(err));
  });
}

test('decompressBuffer restores original bytes', async () => {
  const dec = new GzipStreamDecompressor();
  const original = Buffer.from('hello gzip world', 'utf8');
  const compressed = gzipSync(original);
  const result = await dec.decompressBuffer(compressed);
  assert.deepEqual(result, original);
});

test('decompressBuffer handles empty payload', async () => {
  const dec = new GzipStreamDecompressor();
  const compressed = gzipSync(Buffer.alloc(0));
  const result = await dec.decompressBuffer(compressed);
  assert.deepEqual(result, Buffer.alloc(0));
});

test('decompressBuffer handles binary data', async () => {
  const dec = new GzipStreamDecompressor();
  const original = Buffer.from(
    Array.from({ length: 1024 }, (_, i) => (i * 37) & 0xff)
  );
  const compressed = gzipSync(original);
  const result = await dec.decompressBuffer(compressed);
  assert.deepEqual(result, original);
});

test('decompressBuffer rejects non-Buffer input', async () => {
  const dec = new GzipStreamDecompressor();
  await assert.rejects(
    () => dec.decompressBuffer('not a buffer'),
    { name: 'TypeError' }
  );
});

test('decompressBuffer rejects truncated input', async () => {
  const dec = new GzipStreamDecompressor();
  const original = Buffer.from(
    'this is long enough that gzip will produce more than ten bytes of output'
  );
  const compressed = gzipSync(original);
  const truncated = compressed.subarray(0, compressed.length - 10);
  await assert.rejects(() => dec.decompressBuffer(truncated));
});

test('decompressBuffer rejects corrupted input', async () => {
  const dec = new GzipStreamDecompressor();
  const original = Buffer.from('hello world');
  const compressed = gzipSync(original);
  // Flip a byte in the middle of the deflate stream body.
  const corrupted = Buffer.from(compressed);
  corrupted[10] ^= 0xff;
  await assert.rejects(() => dec.decompressBuffer(corrupted));
});

test('decompressBuffer rejects non-gzip input', async () => {
  const dec = new GzipStreamDecompressor();
  await assert.rejects(() => dec.decompressBuffer(Buffer.from('not gzip')));
});

test('decompressStream restores original bytes', async () => {
  const dec = new GzipStreamDecompressor();
  const original = Buffer.from('streamed gzip payload', 'utf8');
  const compressed = gzipSync(original);
  const stream = dec.decompressStream();
  stream.end(compressed);
  const result = await collectStream(stream);
  assert.deepEqual(result, original);
});

test('decompressStream handles empty payload', async () => {
  const dec = new GzipStreamDecompressor();
  const compressed = gzipSync(Buffer.alloc(0));
  const stream = dec.decompressStream();
  stream.end(compressed);
  const result = await collectStream(stream);
  assert.deepEqual(result, Buffer.alloc(0));
});

test('decompressStream handles binary data', async () => {
  const dec = new GzipStreamDecompressor();
  const original = Buffer.from(
    Array.from({ length: 1024 }, (_, i) => (i * 37) & 0xff)
  );
  const compressed = gzipSync(original);
  const stream = dec.decompressStream();
  stream.end(compressed);
  const result = await collectStream(stream);
  assert.deepEqual(result, original);
});

test('decompressStream propagates errors on corrupted input', async () => {
  const dec = new GzipStreamDecompressor();
  const original = Buffer.from('hello world');
  const compressed = gzipSync(original);
  const corrupted = Buffer.from(compressed);
  corrupted[10] ^= 0xff;
  const stream = dec.decompressStream();
  stream.end(corrupted);
  await assert.rejects(() => collectStream(stream));
});

test('decompressStream propagates errors on non-gzip input', async () => {
  const dec = new GzipStreamDecompressor();
  const stream = dec.decompressStream();
  stream.end(Buffer.from('plain text'));
  await assert.rejects(() => collectStream(stream));
});

test('decompressStream supports incremental writes', async () => {
  const dec = new GzipStreamDecompressor();
  const original = Buffer.from(
    'incremental ' + 'data '.repeat(50) + 'payload'
  );
  const compressed = gzipSync(original);
  const stream = dec.decompressStream();
  const half = Math.floor(compressed.length / 2);
  stream.write(compressed.subarray(0, half));
  stream.end(compressed.subarray(half));
  const result = await collectStream(stream);
  assert.deepEqual(result, original);
});
