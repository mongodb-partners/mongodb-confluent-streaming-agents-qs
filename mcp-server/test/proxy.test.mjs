// Tests for mcp-server/proxy.mjs — Flink/MCP content-type compat shim.
//
// Strategy: spin up a fake upstream server on a random port, then start the
// proxy (also on a random port) by setting env vars TARGET_PORT and LISTEN_PORT
// (the proxy reads them when injected via the test harness — see proxy.mjs).
//
// Covers:
//   - upstream connection refused → 502 JSON
//   - upstream hangs > timeout → proxy aborts
//   - text/plain → rewritten to application/json
//   - application/problem+json → rewritten
//   - no Content-Type → rewritten
//   - application/json → passthrough unchanged
//   - text/event-stream → passthrough (SSE)
//   - Accept header injection — proxy always sends application/json,text/event-stream

import { test, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { createServer, request as httpRequest } from 'node:http';
import { startProxy } from '../proxy.mjs';

// ─── Helpers ────────────────────────────────────────────────────────────────

function listenOnRandomPort(handler) {
  return new Promise((resolve) => {
    const srv = createServer(handler);
    srv.listen(0, '127.0.0.1', () => {
      resolve({ srv, port: srv.address().port });
    });
  });
}

function closeServer(srv) {
  return new Promise((resolve) => srv.close(() => resolve()));
}

function getJson(port, path = '/mcp', body = '') {
  return new Promise((resolve, reject) => {
    const req = httpRequest({
      hostname: '127.0.0.1', port, path,
      method: body ? 'POST' : 'GET',
      headers: body ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) } : {},
    }, (res) => {
      let data = '';
      res.on('data', (c) => { data += c; });
      res.on('end', () => resolve({ statusCode: res.statusCode, headers: res.headers, body: data }));
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

// ─── State per test ─────────────────────────────────────────────────────────

let upstream;     // fake MCP server
let proxy;        // proxy under test
let upstreamPort;
let proxyPort;

beforeEach(() => {
  upstream = null;
  proxy = null;
});

afterEach(async () => {
  if (proxy) await new Promise((r) => proxy.close(() => r()));
  if (upstream) await closeServer(upstream);
});

// ─── upstream refused → JSON 502 ──────────────────────────────

test('upstream connection refused → 502 with application/json', async () => {
  // No upstream server. Pick a port we know is closed.
  const closedPort = 65530;  // unlikely to be open
  proxy = await startProxy({ targetPort: closedPort, listenPort: 0 });
  proxyPort = proxy.address().port;

  const res = await getJson(proxyPort);
  assert.equal(res.statusCode, 502, 'expected 502 on upstream refused');
  assert.match(res.headers['content-type'] || '', /^application\/json/,
               `expected Content-Type application/json, got ${res.headers['content-type']}`);
  const parsed = JSON.parse(res.body);
  assert.ok(parsed.error, 'response body must be JSON with an `error` field');
});

// ─── upstream hang → timeout abort ────────────────────────────

test('upstream hangs → proxy aborts with structured JSON', async () => {
  // Upstream that never responds.
  ({ srv: upstream, port: upstreamPort } = await listenOnRandomPort((req, res) => {
    // do nothing — leak the request
  }));
  proxy = await startProxy({ targetPort: upstreamPort, listenPort: 0, timeoutMs: 500 });
  proxyPort = proxy.address().port;

  const start = Date.now();
  const res = await getJson(proxyPort);
  const elapsed = Date.now() - start;

  assert.ok(elapsed < 2000, `expected timeout < 2s, got ${elapsed}ms`);
  // 504 (Gateway Timeout) or 502 (Bad Gateway) both acceptable.
  assert.ok([502, 504].includes(res.statusCode), `expected 502 or 504, got ${res.statusCode}`);
  assert.match(res.headers['content-type'] || '', /^application\/json/);
  const parsed = JSON.parse(res.body);
  assert.ok(parsed.error, 'must be JSON error envelope');
});

// ─── text/plain → rewritten ───────────────────────────────────

test('upstream text/plain → response Content-Type application/json', async () => {
  ({ srv: upstream, port: upstreamPort } = await listenOnRandomPort((req, res) => {
    res.writeHead(202, { 'Content-Type': 'text/plain; charset=UTF-8' });
    res.end('Accepted');
  }));
  proxy = await startProxy({ targetPort: upstreamPort, listenPort: 0 });
  proxyPort = proxy.address().port;

  const res = await getJson(proxyPort);
  assert.equal(res.statusCode, 202);
  assert.match(res.headers['content-type'] || '', /^application\/json/,
               `text/plain must be rewritten; got ${res.headers['content-type']}`);
});

// ─── application/problem+json → rewritten ─────────────────────

test('upstream application/problem+json → rewritten to application/json', async () => {
  ({ srv: upstream, port: upstreamPort } = await listenOnRandomPort((req, res) => {
    res.writeHead(400, { 'Content-Type': 'application/problem+json' });
    res.end('{"type":"about:blank","title":"Bad Request"}');
  }));
  proxy = await startProxy({ targetPort: upstreamPort, listenPort: 0 });
  proxyPort = proxy.address().port;

  const res = await getJson(proxyPort);
  assert.equal(res.statusCode, 400);
  assert.match(res.headers['content-type'] || '', /^application\/json/);
});

// ─── no Content-Type → rewritten ──────────────────────────────

test('upstream omits Content-Type → proxy forces application/json', async () => {
  ({ srv: upstream, port: upstreamPort } = await listenOnRandomPort((req, res) => {
    res.writeHead(204);  // No Content; no Content-Type
    res.end();
  }));
  proxy = await startProxy({ targetPort: upstreamPort, listenPort: 0 });
  proxyPort = proxy.address().port;

  const res = await getJson(proxyPort);
  assert.equal(res.statusCode, 204);
  assert.match(res.headers['content-type'] || '', /^application\/json/);
});

// ─── application/json → passthrough ───────────────────────────

test('upstream application/json → passthrough unchanged', async () => {
  ({ srv: upstream, port: upstreamPort } = await listenOnRandomPort((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end('{"jsonrpc":"2.0","id":1,"result":{}}');
  }));
  proxy = await startProxy({ targetPort: upstreamPort, listenPort: 0 });
  proxyPort = proxy.address().port;

  const res = await getJson(proxyPort);
  assert.equal(res.statusCode, 200);
  assert.match(res.headers['content-type'] || '', /^application\/json/);
  // Body must be untouched
  assert.equal(res.body, '{"jsonrpc":"2.0","id":1,"result":{}}');
});

// ─── text/event-stream → passthrough (SSE) ────────────────────

test('upstream text/event-stream → passthrough unchanged', async () => {
  ({ srv: upstream, port: upstreamPort } = await listenOnRandomPort((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    res.write('data: {"event":1}\n\n');
    res.end();
  }));
  proxy = await startProxy({ targetPort: upstreamPort, listenPort: 0 });
  proxyPort = proxy.address().port;

  const res = await getJson(proxyPort);
  assert.equal(res.statusCode, 200);
  assert.match(res.headers['content-type'] || '', /^text\/event-stream/,
               'SSE must passthrough unchanged');
});

// ─── Accept header injection ──────────────────────────────────

test('proxy injects Accept: application/json, text/event-stream', async () => {
  let observedAccept = null;
  ({ srv: upstream, port: upstreamPort } = await listenOnRandomPort((req, res) => {
    observedAccept = req.headers['accept'] || '(missing)';
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end('{}');
  }));
  proxy = await startProxy({ targetPort: upstreamPort, listenPort: 0 });
  proxyPort = proxy.address().port;

  await getJson(proxyPort);
  assert.ok(observedAccept.includes('application/json'),
            `Accept must include application/json; got ${observedAccept}`);
  assert.ok(observedAccept.includes('text/event-stream'),
            `Accept must include text/event-stream; got ${observedAccept}`);
});

// ─── Request body size cap ────────────────────────────────────

test('oversized request body → 413 and upstream never contacted', async () => {
  let upstreamHits = 0;
  ({ srv: upstream, port: upstreamPort } = await listenOnRandomPort((req, res) => {
    upstreamHits += 1;
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end('{}');
  }));
  // Tiny cap so the test payload trips it.
  proxy = await startProxy({ targetPort: upstreamPort, listenPort: 0, maxBodyBytes: 64 });
  proxyPort = proxy.address().port;

  const big = JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'x', params: 'A'.repeat(500) });
  const res = await getJson(proxyPort, '/mcp', big);
  assert.equal(res.statusCode, 413, 'oversized body must be rejected with 413');
  assert.match(res.headers['content-type'] || '', /^application\/json/);
  assert.equal(upstreamHits, 0, 'upstream must NOT be contacted for an oversized body');
});

test('body within cap is proxied normally', async () => {
  ({ srv: upstream, port: upstreamPort } = await listenOnRandomPort((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end('{"ok":true}');
  }));
  proxy = await startProxy({ targetPort: upstreamPort, listenPort: 0, maxBodyBytes: 4096 });
  proxyPort = proxy.address().port;

  const body = JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'ping' });
  const res = await getJson(proxyPort, '/mcp', body);
  assert.equal(res.statusCode, 200);
});

// ─── Hop-by-hop header stripping ──────────────────────────────

test('hop-by-hop headers are not forwarded upstream', async () => {
  let forwarded = null;
  ({ srv: upstream, port: upstreamPort } = await listenOnRandomPort((req, res) => {
    forwarded = { ...req.headers };
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end('{}');
  }));
  proxy = await startProxy({ targetPort: upstreamPort, listenPort: 0 });
  proxyPort = proxy.address().port;

  await new Promise((resolve, reject) => {
    const req = httpRequest({
      hostname: '127.0.0.1', port: proxyPort, path: '/mcp', method: 'GET',
      headers: { 'X-Keep': 'yes', 'Upgrade': 'websocket', 'Keep-Alive': 'timeout=5' },
    }, (res) => { res.on('data', () => {}); res.on('end', resolve); });
    req.on('error', reject);
    req.end();
  });

  assert.equal(forwarded['upgrade'], undefined, 'upgrade (hop-by-hop) must be stripped');
  assert.equal(forwarded['keep-alive'], undefined, 'keep-alive (hop-by-hop) must be stripped');
  assert.equal(forwarded['x-keep'], 'yes', 'non-hop-by-hop headers must pass through');
});
