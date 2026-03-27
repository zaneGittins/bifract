// Bifract k6 load test script.
//
// Prerequisites:
//   brew install k6   (or https://k6.io/docs/get-started/installation/)
//
// Usage:
//   # Ingest-only (default):
//   BIFRACT_URL=http://localhost:8080 BIFRACT_INGEST_TOKEN=<token> k6 run scripts/loadtest.js
//
//   # Query-only:
//   BIFRACT_URL=http://localhost:8080 BIFRACT_USER=admin BIFRACT_PASS=bifract \
//     k6 run -e TEST=query scripts/loadtest.js
//
//   # Both:
//   BIFRACT_URL=http://localhost:8080 BIFRACT_INGEST_TOKEN=<token> \
//     BIFRACT_USER=admin BIFRACT_PASS=bifract \
//     k6 run -e TEST=both scripts/loadtest.js
//
//   # Override VUs / duration:
//   k6 run --vus 50 --duration 5m scripts/loadtest.js

import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const BASE_URL = __ENV.BIFRACT_URL || "http://localhost:8080";
const INGEST_TOKEN = __ENV.BIFRACT_INGEST_TOKEN || "";
const USER = __ENV.BIFRACT_USER || "admin";
const PASS = __ENV.BIFRACT_PASS || "bifract";
const TEST_MODE = (__ENV.TEST || "ingest").toLowerCase(); // ingest | query | both
const BATCH_SIZE = parseInt(__ENV.BATCH_SIZE || "100", 10);
const FRACTAL_ID = __ENV.BIFRACT_FRACTAL_ID || "";

// ---------------------------------------------------------------------------
// Custom metrics
// ---------------------------------------------------------------------------

const ingestLogs = new Counter("bifract_ingest_logs_total");
const ingestErrors = new Counter("bifract_ingest_errors_total");
const ingest429s = new Counter("bifract_ingest_429s_total");
const ingestLatency = new Trend("bifract_ingest_latency_ms", true);

const queryErrors = new Counter("bifract_query_errors_total");
const queryLatency = new Trend("bifract_query_latency_ms", true);
const queryRows = new Trend("bifract_query_rows_returned", true);

// ---------------------------------------------------------------------------
// Default test options
// ---------------------------------------------------------------------------

export const options = {
  scenarios: {
    ramp: {
      executor: "ramping-vus",
      startVUs: 1,
      stages: [
        { duration: "30s", target: 10 },
        { duration: "1m", target: 25 },
        { duration: "2m", target: 50 },
        { duration: "1m", target: 50 }, // steady state
        { duration: "30s", target: 0 },
      ],
    },
  },
  thresholds: {
    bifract_ingest_latency_ms: ["p(95)<2000"],
    bifract_query_latency_ms: ["p(95)<5000"],
    "http_req_failed{name:ingest}": ["rate<0.05"],
    "http_req_failed{name:query}": ["rate<0.05"],
  },
};

// ---------------------------------------------------------------------------
// Session cookie (lazy-initialized for query tests)
// ---------------------------------------------------------------------------

let sessionCookie = null;

function login() {
  const res = http.post(
    `${BASE_URL}/api/v1/auth/login`,
    JSON.stringify({ username: USER, password: PASS }),
    { headers: { "Content-Type": "application/json" } }
  );

  const cookies = res.cookies;
  if (cookies && cookies["session_id"]) {
    sessionCookie = cookies["session_id"][0].value;
  }

  if (!sessionCookie) {
    console.error(`Login failed (status ${res.status}): ${res.body}`);
  }
  return sessionCookie;
}

// ---------------------------------------------------------------------------
// Log generation
// ---------------------------------------------------------------------------

const LOG_LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"];
const SERVICES = [
  "api-gateway",
  "auth-service",
  "billing",
  "inventory",
  "notification",
  "scheduler",
  "worker",
];
const MESSAGES = [
  "request completed successfully",
  "cache miss, fetching from database",
  "connection pool exhausted, waiting for available connection",
  "rate limit exceeded for client",
  "health check passed",
  "failed to parse request body",
  "timeout waiting for downstream response",
  "retry attempt",
  "user session expired",
  "TLS handshake completed",
  "disk usage above threshold",
  "query execution completed",
  "batch insert committed",
  "certificate renewal triggered",
  "configuration reload detected",
];

function randomItem(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function generateLog() {
  const level = randomItem(LOG_LEVELS);
  const service = randomItem(SERVICES);
  const msg = randomItem(MESSAGES);
  const statusCode = level === "ERROR" ? 500 : level === "WARN" ? 429 : 200;

  return {
    timestamp: new Date().toISOString(),
    level: level,
    service: service,
    message: `[${service}] ${msg}`,
    host: `prod-${service}-${Math.floor(Math.random() * 10)}`,
    status_code: statusCode,
    duration_ms: Math.floor(Math.random() * 2000),
    request_id: `req-${Date.now()}-${Math.floor(Math.random() * 100000)}`,
    user_agent: "k6-loadtest/1.0",
    source_ip: `10.${Math.floor(Math.random() * 256)}.${Math.floor(Math.random() * 256)}.${Math.floor(Math.random() * 256)}`,
  };
}

function generateBatch(size) {
  const batch = [];
  for (let i = 0; i < size; i++) {
    batch.push(generateLog());
  }
  return batch;
}

// ---------------------------------------------------------------------------
// Ingest test
// ---------------------------------------------------------------------------

function runIngest() {
  const batch = generateBatch(BATCH_SIZE);
  const payload = JSON.stringify(batch);

  const res = http.post(`${BASE_URL}/api/v1/ingest`, payload, {
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${INGEST_TOKEN}`,
    },
    tags: { name: "ingest" },
    responseCallback: http.expectedStatuses(200, 429),
  });

  ingestLatency.add(res.timings.duration);

  if (res.status === 429) {
    ingest429s.add(1);
    // Respect Retry-After header
    const retryAfter = parseInt(res.headers["Retry-After"] || "2", 10);
    sleep(retryAfter);
    return;
  }

  const ok = check(res, {
    "ingest status 200": (r) => r.status === 200,
    "ingest success": (r) => {
      try {
        return JSON.parse(r.body).success === true;
      } catch (e) {
        return false;
      }
    },
  });

  if (ok) {
    ingestLogs.add(BATCH_SIZE);
  } else {
    ingestErrors.add(1);
  }
}

// ---------------------------------------------------------------------------
// Query test
// ---------------------------------------------------------------------------

const QUERIES = [
  // Full-text search
  `"timeout"`,
  `"cache miss"`,
  // Field filter
  `level = "ERROR"`,
  `service = "api-gateway"`,
  `status_code >= 400`,
  // Aggregation
  `level = "ERROR" | stats count() by service`,
  `* | stats avg(duration_ms), max(duration_ms) by level`,
  `* | stats count() by host | sort count desc | head 10`,
  // Combined
  `service = "billing" AND level != "DEBUG"`,
  `duration_ms > 1000 | stats count() by service`,
];

function runQuery() {
  if (!sessionCookie) {
    if (!login()) {
      queryErrors.add(1);
      sleep(5);
      return;
    }
  }

  const now = new Date();
  const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);

  const body = {
    query: randomItem(QUERIES),
    start: oneHourAgo.toISOString(),
    end: now.toISOString(),
  };

  if (FRACTAL_ID) {
    body.fractal_id = FRACTAL_ID;
  }

  const res = http.post(`${BASE_URL}/api/v1/query`, JSON.stringify(body), {
    headers: { "Content-Type": "application/json" },
    cookies: { session_id: sessionCookie },
    tags: { name: "query" },
  });

  queryLatency.add(res.timings.duration);

  // Re-login on 401
  if (res.status === 401) {
    sessionCookie = null;
    return;
  }

  const ok = check(res, {
    "query status 200": (r) => r.status === 200,
    "query success": (r) => {
      try {
        return JSON.parse(r.body).success === true;
      } catch (e) {
        return false;
      }
    },
  });

  if (ok) {
    try {
      const data = JSON.parse(res.body);
      queryRows.add(data.count || 0);
    } catch (e) {
      // ignore parse errors for metrics
    }
  } else {
    queryErrors.add(1);
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

export default function () {
  switch (TEST_MODE) {
    case "query":
      runQuery();
      break;
    case "both":
      if (Math.random() < 0.7) {
        runIngest();
      } else {
        runQuery();
      }
      break;
    default:
      runIngest();
      break;
  }

  // Small pause between iterations to avoid tight-looping
  sleep(0.1 + Math.random() * 0.2);
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------

export function handleSummary(data) {
  const out = [];
  out.push("\n=== Bifract Load Test Summary ===\n");

  const get = (name) => data.metrics[name];

  const val = (m, key) => m && m.values && m.values[key] != null ? m.values[key] : null;

  const il = get("bifract_ingest_logs_total");
  if (val(il, "count") !== null) {
    out.push(`Logs ingested:    ${il.values.count}`);
  }
  const ie = get("bifract_ingest_errors_total");
  if (val(ie, "count") !== null) {
    out.push(`Ingest errors:    ${ie.values.count}`);
  }
  const i4 = get("bifract_ingest_429s_total");
  if (val(i4, "count") !== null) {
    out.push(`Ingest 429s:      ${i4.values.count}`);
  }
  const ilt = get("bifract_ingest_latency_ms");
  if (val(ilt, "p(50)") !== null) {
    out.push(
      `Ingest latency:   p50=${ilt.values["p(50)"].toFixed(0)}ms  p95=${ilt.values["p(95)"].toFixed(0)}ms  p99=${ilt.values["p(99)"].toFixed(0)}ms`
    );
  }

  const ql = get("bifract_query_latency_ms");
  if (val(ql, "p(50)") !== null) {
    out.push(
      `Query latency:    p50=${ql.values["p(50)"].toFixed(0)}ms  p95=${ql.values["p(95)"].toFixed(0)}ms  p99=${ql.values["p(99)"].toFixed(0)}ms`
    );
  }
  const qe = get("bifract_query_errors_total");
  if (val(qe, "count") !== null) {
    out.push(`Query errors:     ${qe.values.count}`);
  }
  const qr = get("bifract_query_rows_returned");
  if (val(qr, "avg") !== null) {
    out.push(`Query rows (avg): ${qr.values.avg.toFixed(0)}`);
  }

  out.push("");
  return { stdout: out.join("\n") };
}
