// Reverse proxy that fixes Flink's MCP-client compatibility issues.
//
// Flink's Spring AI MCP Client v0.3.1 has two bugs we work around here:
//   1. It validates Content-Type on every response including 202 / 204 / 4xx.
//      MongoDB MCP Server returns text/plain on these. We force application/json.
//   2. It does not send `Accept: application/json, text/event-stream` (an MCP
//      SDK requirement). We inject it on every request.
//
// error path returns JSON-RPC-shaped 502 with
//   Content-Type: application/json (was text/plain — the exact bug the proxy
//   exists to prevent).
// upstream requests have a 30 s timeout; hangs return a
//   structured 504 JSON error instead of blocking the ALB.
// Content-Type rewrite uses an allow-list, not a deny-list.
//   application/json and text/event-stream pass through; everything else
//   (absent, text/plain, text/html, application/problem+json, application/xml,
//   …) is rewritten to application/json.
//
// Exports:
//   startProxy({ targetPort, listenPort, timeoutMs }) → http.Server
//     Returns an already-listening Server; caller awaits address() then closes
//     it. Listening on listenPort=0 picks an ephemeral port (used by tests).
//
// When run directly (not imported), the module starts the proxy with default
// ports 8000 (upstream) / 8080 (listen) — matching the Docker container.

import { createServer, request as httpRequest } from 'node:http';

const DEFAULT_TARGET_PORT = 8000;
const DEFAULT_LISTEN_PORT = 8080;
const DEFAULT_TIMEOUT_MS = 30_000;
// Cap the buffered request body so a large (or malicious) payload can't be
// held fully in memory before upstream auth even runs. MCP JSON-RPC requests
// are small; 4 MiB is generous. Override with MCP_PROXY_MAX_BODY_BYTES.
const DEFAULT_MAX_BODY_BYTES = Number(
  process.env.MCP_PROXY_MAX_BODY_BYTES || 4 * 1024 * 1024,
);

// Content-Type allow-list. Anything else is rewritten to application/json.
const CT_ALLOW = ['application/json', 'text/event-stream'];

// Hop-by-hop headers (RFC 7230 §6.1) are connection-specific and MUST NOT be
// forwarded by a proxy. Forwarding e.g. `transfer-encoding` alongside our own
// `content-length` produces a framing conflict / request-smuggling surface.
const HOP_BY_HOP_HEADERS = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
]);

function stripHopByHop(headers) {
  const out = {};
  for (const [k, v] of Object.entries(headers)) {
    if (!HOP_BY_HOP_HEADERS.has(k.toLowerCase())) out[k] = v;
  }
  return out;
}

function shouldRewriteContentType(ct) {
  if (!ct) return true;  // missing → force JSON
  const lower = ct.toLowerCase();
  return !CT_ALLOW.some((prefix) => lower.startsWith(prefix));
}

function jsonError(statusCode, message, requestId = null) {
  // JSON-RPC 2.0 shape (per MCP spec). Even when the upstream call never
  // produced a request id, we emit `id: null` which is valid JSON-RPC.
  return JSON.stringify({
    jsonrpc: '2.0',
    id: requestId,
    error: {
      code: statusCode === 504 ? -32001 : -32603,  // timeout / internal
      message,
    },
  });
}

function handleRequest(req, res, { targetPort, timeoutMs, maxBodyBytes }) {
  // Strip hop-by-hop + host, then inject MCP-SDK-required Accept.
  const headers = stripHopByHop(req.headers);
  const originalAccept = headers['accept'];
  headers['accept'] = 'application/json, text/event-stream';
  delete headers['host'];

  console.error(`[proxy] ${req.method} ${req.url} accept=${originalAccept || '(none)'}`);

  // Buffer as Buffers (not string concat, which corrupts multi-byte UTF-8 and
  // binary), enforcing the size cap as chunks arrive.
  const chunks = [];
  let received = 0;
  let overflowed = false;
  let parsedRequestId = null;
  req.on('data', (chunk) => {
    if (overflowed) return;
    received += chunk.length;
    if (received > maxBodyBytes) {
      overflowed = true;
      const body = jsonError(413, `Request body exceeds ${maxBodyBytes} bytes`, null);
      if (!res.headersSent) {
        res.writeHead(413, {
          'content-type': 'application/json',
          'content-length': Buffer.byteLength(body),
        });
      }
      res.end(body);
      req.destroy();
      return;
    }
    chunks.push(chunk);
  });
  req.on('end', () => {
    if (overflowed) return;  // 413 already sent
    const reqBody = Buffer.concat(chunks);
    if (reqBody.length) {
      try {
        const parsed = JSON.parse(reqBody);
        parsedRequestId = parsed.id ?? null;
        console.error(`[proxy] request: method=${parsed.method} id=${parsed.id}`);
      } catch {
        // Body isn't JSON — fine; just lose the id binding.
      }
    }

    const proxyReq = httpRequest(
      {
        hostname: '127.0.0.1',
        port: targetPort,
        path: req.url,
        method: req.method,
        headers: { ...headers, 'content-length': reqBody.length },
      },
      (proxyRes) => {
        // Strip hop-by-hop headers from the upstream response too, so we don't
        // relay connection-specific framing back to the client.
        const responseHeaders = stripHopByHop(proxyRes.headers);
        const upstreamCt = responseHeaders['content-type'];

        // allow-list rewrite.
        if (shouldRewriteContentType(upstreamCt)) {
          responseHeaders['content-type'] = 'application/json';
        }

        console.error(`[proxy] response: ${proxyRes.statusCode} upstream-ct=${upstreamCt || '(none)'}`);
        res.writeHead(proxyRes.statusCode, responseHeaders);
        proxyRes.pipe(res);
      }
    );

    // upstream timeout.
    let timedOut = false;
    proxyReq.setTimeout(timeoutMs, () => {
      timedOut = true;
      proxyReq.destroy(new Error('upstream timeout'));
    });

    proxyReq.on('error', (err) => {
      console.error(`[proxy] error: ${err.message}`);
      if (res.headersSent) {
        // We already started streaming the response — best we can do is end it.
        try { res.end(); } catch { /* swallow */ }
        return;
      }
      // structured JSON 502 with application/json.
      const statusCode = timedOut ? 504 : 502;
      const body = jsonError(statusCode, `Proxy error: ${err.message}`, parsedRequestId);
      res.writeHead(statusCode, {
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(body),
      });
      res.end(body);
    });

    proxyReq.write(reqBody);
    proxyReq.end();
  });
}

export function startProxy({
  targetPort = DEFAULT_TARGET_PORT,
  listenPort = DEFAULT_LISTEN_PORT,
  timeoutMs = DEFAULT_TIMEOUT_MS,
  maxBodyBytes = DEFAULT_MAX_BODY_BYTES,
} = {}) {
  const server = createServer((req, res) =>
    handleRequest(req, res, { targetPort, timeoutMs, maxBodyBytes }));

  return new Promise((resolve) => {
    server.listen(listenPort, '0.0.0.0', () => {
      const port = server.address().port;
      console.error(`MCP proxy listening on :${port} -> :${targetPort} (timeout=${timeoutMs}ms)`);
      resolve(server);
    });
  });
}

// When invoked directly, start with default ports. When imported (e.g. by
// tests), the caller drives startProxy().
const isDirectInvocation =
  import.meta.url === `file://${process.argv[1]}` ||
  process.argv[1]?.endsWith('proxy.mjs');

if (isDirectInvocation) {
  startProxy().catch((err) => {
    console.error(`[proxy] failed to start: ${err.message}`);
    process.exit(1);
  });
}
