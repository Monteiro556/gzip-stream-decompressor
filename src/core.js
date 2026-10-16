/**
 * Core of gzip-stream-decompressor.
 *
 * Wraps Node's zlib.createGunzip in a contract that surfaces decompression
 * errors as rejected promises rather than silent 'error'-event drops, and
 * provides a buffer helper for the common "whole file in memory" case.
 *
 * Why wrap instead of use zlib.gunzipSync directly?
 *  - gunzipSync throws on truncated input, but the thrown error gives no
 *    indication that the cause was truncation rather than corruption.
 *  - A streaming decoder can distinguish EOF-before-trailer (truncation)
 *    from a checksum mismatch (corruption) because it controls the pipeline.
 *  - We use the stream's 'error' event as a weak signal: if the stream emits
 *    error before we see a clean finish, we treat it as corruption; otherwise
 *    we treat a short output as truncation. This is the closest Node's zlib
 *    stream API gets to the distinction without re-implementating inflate.
 */

import { createGunzip } from 'node:zlib';
import { PassThrough } from 'node:stream';

export class GzipStreamDecompressor {
  /**
   * @param {object} [opts]
   * @param {number} [opts.chunkSize] High-water mark for the internal
   *   gunzip transform. Left at the zlib default unless overridden.
   */
  constructor(opts) {
    const o = opts || {};
    this._chunkSize = o.chunkSize;
  }

  /**
   * Create a fresh gunzip transform stream.
   *
   * A new instance is created per call because Node's zlib streams are
   * single-use: once 'close' fires, they cannot be reset.
   *
   * @returns {import('node:zlib').Gunzip}
   */
  _createGunzip() {
    const gunzipOpts = {};
    if (this._chunkSize !== undefined) {
      gunzipOpts.chunkSize = this._chunkSize;
    }
    return createGunzip(gunzipOpts);
  }

  /**
   * Returns a Transform stream that decompresses gzip input.
   *
   * Consumers pipe a gzip source into the returned stream and read plain
   * bytes from it. Errors are emitted via the standard 'error' event;
   * consumers are responsible for attaching a listener, as with any
   * Node stream.
   *
   * @returns {import('node:stream').Transform}
   */
  decompressStream() {
    const gunzip = this._createGunzip();
    const bridge = new PassThrough({
      highWaterMark: this._chunkSize,
    });

    // Forward data and end so consumers only attach to one stream.
    gunzip.on('data', (chunk) => {
      if (!bridge.destroyed) {
        bridge.push(chunk);
      }
    });
    gunzip.on('end', () => {
      if (!bridge.destroyed) {
        bridge.push(null);
      }
    });
    gunzip.on('error', (err) => {
      bridge.destroy(err);
    });

    // The returned bridge exposes .write/.end so callers can feed it bytes.
    const origWrite = bridge.write.bind(bridge);
    const origEnd = bridge.end.bind(bridge);
    bridge.write = (chunk, encoding, cb) => {
      return gunzip.write(chunk, encoding, cb);
    };
    bridge.end = (chunk, encoding, cb) => {
      if (chunk !== undefined) {
        gunzip.end(chunk, encoding, cb);
      } else {
        gunzip.end(cb);
      }
      return false;
    };

    return bridge;
  }

  /**
   * Decompress a whole gzip Buffer and return the plaintext as a Buffer.
   *
   * Resolves on successful decompression. Rejects with a ZlibError on
   * corruption (bad header, bad checksum), and with a custom Error on
   * truncation (input ended before the stream emitted 'end').
   *
   * @param {Buffer} input
   * @returns {Promise<Buffer>}
   */
  decompressBuffer(input) {
    if (!Buffer.isBuffer(input)) {
      return Promise.reject(new TypeError('Input must be a Buffer'));
    }

    return new Promise((resolve, reject) => {
      const gunzip = this._createGunzip();
      const chunks = [];
      let sawError = false;

      gunzip.on('data', (chunk) => {
        chunks.push(chunk);
      });
      gunzip.on('end', () => {
        resolve(Buffer.concat(chunks));
      });
      gunzip.on('error', (err) => {
        sawError = true;
        // Node's gunzip emits ZlibError for bad header or checksum.
        reject(err);
      });

      gunzip.on('close', () => {
        if (!sawError && chunks.length === 0 && input.length > 0) {
          // Stream closed without error or output: treat as truncation.
          reject(new Error('Truncated gzip input'));
        }
      });

      gunzip.end(input);
    });
  }
}
