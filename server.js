import express from "express";
import rateLimit from "express-rate-limit";
import { spawn } from "node:child_process";
import os from "node:os";
import { createHash, timingSafeEqual } from "node:crypto";
import { MsEdgeTTS, OUTPUT_FORMAT } from "msedge-tts";

// ---------- Config (override via environment variables) ----------
const VERSION = "1.7.0-sequential";
const PORT = Number(process.env.PORT) || 3000;
const API_KEY = process.env.API_KEY || "";                    // empty = auth disabled
const MAX_TEXT = Number(process.env.MAX_TEXT) || 0;           // characters per request, 0 = unlimited
const JSON_LIMIT = process.env.JSON_LIMIT || "5mb";           // max request body size
const RATE_LIMIT = process.env.RATE_LIMIT === undefined ? 30 : Number(process.env.RATE_LIMIT); // requests/min/IP, 0 = off
const MAX_TIMEOUT_MS = Number(process.env.MAX_TIMEOUT_MS) || 600_000; // hard ceiling for one request (10 min)
const CACHE_MAX_BYTES = 2 * 1024 * 1024;                          // clips larger than this are not cached
const MAX_CONCURRENT = Number(process.env.MAX_CONCURRENT) || 4;
const TIMEOUT_MS = Number(process.env.TIMEOUT_MS) || 60_000;   // base timeout; grows with the number of chunks
const CACHE_MAX = Number(process.env.CACHE_MAX) || 200;       // cached clips
const CHUNK_MAX = Number(process.env.CHUNK_MAX) || 35;        // max characters per Edge request
const MIN_CHUNK = 10;                                         // never split below this many characters
const CHUNK_PARALLEL = Number(process.env.CHUNK_PARALLEL) || 1; // 1 = one chunk at a time (gentlest on Edge)
const DEBUG = process.env.DEBUG_TTS === "1";                  // verbose msedge-tts protocol logging
const DEFAULT_VOICE = process.env.DEFAULT_VOICE || "km-KH-SreymomNeural";
const VOICE_RE = /^[a-z]{2,3}-[A-Z]{2}-\w+Neural$/;

const SOURCE_FORMAT = OUTPUT_FORMAT.AUDIO_24KHZ_48KBITRATE_MONO_MP3;
// Opus encoder effort: 0 = fastest ... 10 = best (ffmpeg's default is 10). On a tiny CPU like
// Render's free plan, 3 is ~45% cheaper than 10 with no audible difference for speech.
const FFMPEG_COMPLEXITY = String(Math.min(10, Math.max(0, parseInt(process.env.FFMPEG_COMPLEXITY ?? "3", 10) || 0)));
const FFMPEG_ARGS = [
  "-hide_banner", "-loglevel", "error", "-nostdin",
  "-f", "mp3", "-i", "pipe:0",          // input format is known: skip format probing
  "-vn", "-sn", "-dn", "-map", "0:a:0", // audio only
  "-map_metadata", "-1",                // strip tags
  "-threads", "1",                      // no thread overhead on a fractional CPU
  "-c:a", "libopus",
  "-application", "voip",               // tuned for speech
  "-b:a", "32k",
  "-compression_level", FFMPEG_COMPLEXITY,
  "-ar", "48000",
  "-ac", "1",
  "-f", "ogg",
  "pipe:1",
];

// ---------- Helpers ----------
const sha = (s) => createHash("sha256").update(s).digest();
const safeEqual = (a, b) => timingSafeEqual(sha(a), sha(b));
const authorized = (req) => !API_KEY || safeEqual(String(req.get("x-api-key") || ""), API_KEY);
const len = (s) => [...s].length; // length in code points (Khmer-safe, no split surrogates)

// "1.25x" -> "+25%", "0.75x" -> "-25%", clamped to 0.5x–2x
function parseRate(speed) {
  const n = Math.min(2, Math.max(0.5, parseFloat(speed) || 1));
  const pct = Math.round((n - 1) * 100);
  return `${pct >= 0 ? "+" : ""}${pct}%`;
}

// msedge-tts pastes the text straight into SSML/XML without escaping it.
function toSsmlText(text) {
  return text
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\uFFFE\uFFFF]/g, "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

// Edge's server closes the socket on longer inputs for some voices (reported for Thai and
// seen here with Khmer): short requests pass, longer ones fail. So we send short chunks.
let wordSegmenter = null;
try { wordSegmenter = new Intl.Segmenter("km", { granularity: "word" }); } catch {}

function cutByLength(str, max) {
  // Prefer word boundaries (Khmer has few spaces); fall back to a plain code-point cut.
  const units = wordSegmenter ? [...wordSegmenter.segment(str)].map((s) => s.segment) : [...str];
  const out = [];
  let cur = "";
  for (const u of units) {
    if (cur && len(cur) + len(u) > max) { out.push(cur); cur = ""; }
    cur += u;
    while (len(cur) > max) { const cp = [...cur]; out.push(cp.slice(0, max).join("")); cur = cp.slice(max).join(""); }
  }
  if (cur) out.push(cur);
  return out;
}

function splitText(text, max = CHUNK_MAX) {
  const sentences = text.replace(/\r/g, "")
    .split(/(?<=[។៕.!?\n])\s*/u)
    .map((s) => s.trim())
    .filter(Boolean);

  const pieces = [];
  for (const s of sentences) {
    if (len(s) <= max) { pieces.push(s); continue; }
    // too long: break on commas/spaces first, then by words/length
    let cur = "";
    for (const part of s.split(/(?<=[,;:៖])\s*|\s+/u).filter(Boolean)) {
      if (len(part) > max) {
        if (cur) { pieces.push(cur); cur = ""; }
        pieces.push(...cutByLength(part, max));
      } else if (cur && len(cur) + 1 + len(part) > max) {
        pieces.push(cur); cur = part;
      } else {
        cur = cur ? `${cur} ${part}` : part;
      }
    }
    if (cur) pieces.push(cur);
  }

  // merge neighbouring short pieces back together up to `max`
  const merged = [];
  for (const p of pieces) {
    const last = merged[merged.length - 1];
    if (last !== undefined && len(last) + 1 + len(p) <= max) merged[merged.length - 1] = `${last} ${p}`;
    else merged.push(p);
  }
  // drop chunks with nothing speakable (only punctuation/symbols)
  return merged.filter((p) => /[\p{L}\p{N}\p{M}]/u.test(p));
}

// Tiny LRU cache (Map preserves insertion order)
const cache = new Map();
function cacheGet(key) {
  const hit = cache.get(key);
  if (hit) { cache.delete(key); cache.set(key, hit); }
  return hit;
}
function cacheSet(key, buf) {
  cache.set(key, buf);
  if (cache.size > CACHE_MAX) cache.delete(cache.keys().next().value);
}

async function mapLimit(items, limit, fn, signal) {
  const results = new Array(items.length);
  let next = 0, failed = false;
  const worker = async (w) => {
    while (!failed && !signal.aborted && next < items.length) {
      const i = next++;
      try { results[i] = await fn(items[i], i, w); } catch (e) { failed = true; throw e; }
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, (_, w) => worker(w)));
  if (signal.aborted) throw signal.reason;
  return results;
}

// Collect an Edge audio stream into one MP3 buffer (and always detach the abort listener)
function collect(stream, signal) {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(signal.reason);
    const chunks = [];
    const finish = (fn, value) => { signal.removeEventListener("abort", onAbort); fn(value); };
    const onAbort = () => finish(reject, signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    stream.on("data", (c) => chunks.push(c));
    stream.on("error", (e) => finish(reject, e));
    stream.on("end", () => {
      const buf = Buffer.concat(chunks);
      buf.length ? finish(resolve, buf) : finish(reject, new Error("No audio data received"));
    });
  });
}

// One WebSocket to Edge, reused for the chunks of a request (fewer handshakes = fewer
// connections for Microsoft to throttle). After any failure the socket is discarded.
class EdgeSession {
  constructor(voice) { this.voice = voice; this.tts = null; }
  async speak(text, rate, signal) {
    try {
      if (!this.tts) {
        this.tts = new MsEdgeTTS({ enableLogger: DEBUG, synthesisTimeoutMs: 20_000 });
        await this.tts.setMetadata(this.voice, SOURCE_FORMAT);
      }
      const { audioStream } = this.tts.toStream(toSsmlText(text), { rate });
      return await collect(audioStream, signal);
    } catch (err) {
      this.close();
      throw err;
    }
  }
  close() {
    try { this.tts?.close(); } catch {}
    this.tts = null;
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const speakable = (t) => /[\p{L}\p{N}\p{M}]/u.test(t);

// Split a string near its middle, on a word boundary when possible
function halve(text) {
  const units = wordSegmenter ? [...wordSegmenter.segment(text)].map((x) => x.segment) : [...text];
  const half = len(text) / 2;
  let acc = "", i = 0;
  for (; i < units.length; i++) {
    if (acc && len(acc) + len(units[i]) > half) break;
    acc += units[i];
  }
  const a = acc.trim(), b = units.slice(i).join("").trim();
  return a && b && speakable(a) && speakable(b) ? [a, b] : null;
}

const chunkLimit = () => CHUNK_MAX;

// Edge sometimes closes the socket mid-request. Retry the same text once on a fresh socket after a
// short pause; only if that also fails, split it in half (at most two levels deep). Splitting
// immediately would multiply connections and make throttling worse.
async function speakChunk(session, text, rate, signal, depth = 0) {
  let lastErr;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      return await session.speak(text, rate, signal);
    } catch (err) {
      lastErr = err;
      if (signal.aborted) throw err;
      console.warn(`Chunk failed (attempt ${attempt}) len=${len(text)} text=${JSON.stringify(text.slice(0, 40))}`);
      if (attempt < 2) await sleep(700);
    }
  }
  const parts = depth < 2 && len(text) > MIN_CHUNK ? halve(text) : null;
  if (!parts) throw lastErr;
  console.warn(`Splitting chunk len=${len(text)} into ${len(parts[0])}+${len(parts[1])}`);
  const bufs = [];
  for (const part of parts) bufs.push(await speakChunk(session, part, rate, signal, depth + 1));
  return Buffer.concat(bufs);
}

// Long-lived-per-request FFmpeg encoder: MP3 chunks are written in as soon as they are ready,
// so encoding overlaps with synthesis instead of waiting for the last chunk.
function startOpus() {
  const ff = spawn("ffmpeg", FFMPEG_ARGS, { stdio: ["pipe", "pipe", "pipe"] });
  // lower priority so encoding never starves the event loop / Edge WebSockets on a tiny CPU
  try { os.setPriority(ff.pid, 10); } catch {}
  const out = [];
  let stderr = "";
  ff.stdout.on("data", (c) => out.push(c));
  ff.stderr.on("data", (d) => { if (stderr.length < 2000) stderr += d; });
  ff.stdin.on("error", () => {}); // EPIPE if ffmpeg exits early; reported via `done`
  const done = new Promise((resolve, reject) => {
    ff.on("error", reject);
    ff.on("close", (code) => {
      const buf = Buffer.concat(out);
      code === 0 && buf.length
        ? resolve(buf)
        : reject(new Error(`ffmpeg exited ${code}: ${stderr.trim() || "no output"}`));
    });
  });
  done.catch(() => {}); // avoid an unhandled rejection before anyone awaits it
  return {
    done,
    write: (buf) => { if (ff.stdin.writable) ff.stdin.write(buf); },
    end: () => ff.stdin.end(),
    kill: () => { if (ff.exitCode === null) ff.kill("SIGKILL"); },
  };
}

async function synthesize(text, voice, rate, signal, chunkMax = chunkLimit(voice)) {
  const chunks = splitText(text, chunkMax);
  if (!chunks.length) throw Object.assign(new Error("Nothing speakable in text"), { status: 400 });

  const stop = new AbortController(); // lets an ffmpeg crash cancel the remaining synthesis
  const sig = AbortSignal.any([signal, stop.signal]);
  const enc = startOpus();
  enc.done.catch((e) => stop.abort(e));
  const sessions = Array.from({ length: Math.min(CHUNK_PARALLEL, chunks.length) }, () => new EdgeSession(voice));

  try {
    const ready = new Map();
    let nextToWrite = 0;
    await mapLimit(chunks, CHUNK_PARALLEL, async (chunk, i, w) => {
      ready.set(i, await speakChunk(sessions[w], chunk, rate, sig));
      // chunks finish out of order; feed ffmpeg strictly in order
      while (ready.has(nextToWrite)) {
        enc.write(ready.get(nextToWrite));
        ready.delete(nextToWrite++);
      }
    }, sig);
    enc.end();
    return { opus: await enc.done, chunks: chunks.length };
  } catch (err) {
    enc.kill();
    throw err;
  } finally {
    sessions.forEach((x) => x.close());
  }
}

// ---------- App ----------
const app = express();
app.disable("x-powered-by");
app.set("trust proxy", 1);
app.use(express.json({ limit: JSON_LIMIT }));

app.get("/", (_req, res) => res.send("Edge TTS + FFmpeg service is live."));
app.get("/health", (_req, res) => res.json({ ok: true, version: VERSION, active, cached: cache.size }));

const limiter = RATE_LIMIT > 0
  ? rateLimit({ windowMs: 60_000, limit: RATE_LIMIT, standardHeaders: true, legacyHeaders: false })
  : (_req, _res, next) => next();

let active = 0;

app.post("/tts", limiter, async (req, res) => {
  if (!authorized(req)) return res.status(401).json({ error: "Unauthorized" });

  const { text, voice = DEFAULT_VOICE, speed = "1.0x" } = req.body ?? {};
  if (typeof text !== "string" || !text.trim()) return res.status(400).json({ error: "Missing text" });
  if (MAX_TEXT && text.length > MAX_TEXT) return res.status(413).json({ error: `Text exceeds ${MAX_TEXT} characters` });
  if (typeof voice !== "string" || !VOICE_RE.test(voice)) return res.status(400).json({ error: "Invalid voice" });

  const rate = parseRate(speed);
  const key = sha(`${voice}\n${rate}\n${text}`).toString("hex");

  const send = (buf, hit) => {
    res.set({
      "Content-Type": "audio/ogg",
      "Content-Length": buf.length,
      "Cache-Control": "no-store",
      "X-Cache": hit ? "HIT" : "MISS",
    });
    res.end(buf);
  };

  const cached = cacheGet(key);
  if (cached) return send(cached, true);

  if (active >= MAX_CONCURRENT) {
    res.set("Retry-After", "2");
    return res.status(503).json({ error: "Server busy, retry shortly" });
  }

  const client = new AbortController();
  res.on("close", () => { if (!res.writableFinished) client.abort(new Error("client closed")); });
  // timeout grows with text length (~1.5 s per chunk), capped by MAX_TIMEOUT_MS
  const estChunks = splitText(text, chunkLimit(voice)).length;
  const timeoutMs = Math.min(MAX_TIMEOUT_MS, TIMEOUT_MS + estChunks * 1500);
  const signal = AbortSignal.any([client.signal, AbortSignal.timeout(timeoutMs)]);

  active++;
  try {
    const { opus } = await synthesize(text, voice, rate, signal);
    if (opus.length <= CACHE_MAX_BYTES) cacheSet(key, opus);
    if (!res.destroyed) send(opus, false);
  } catch (err) {
    console.error(`TTS error: ${err.message} (voice=${voice} rate=${rate} len=${len(text)} start=${JSON.stringify(text.slice(0, 40))})`);
    if (!res.headersSent && !res.destroyed) {
      const status = err.status || (err.name === "TimeoutError" ? 504 : 502);
      res.status(status).json({ error: err.status ? err.message : "TTS failed" });
    }
  } finally {
    active--;
  }
});

// Self-test. Examples:
//   /diag                                   long Khmer sample, default voice, chunked
//   /diag?voice=km-KH-PisethNeural          try the other Khmer voice
//   /diag?chunk=100000                      same text WITHOUT chunking (to compare)
// Send header x-api-key if API_KEY is set.
app.get("/diag", limiter, async (req, res) => {
  if (!authorized(req)) return res.status(401).json({ error: "Unauthorized" });
  const voice = String(req.query.voice || DEFAULT_VOICE);
  if (!VOICE_RE.test(voice)) return res.status(400).json({ error: "Invalid voice" });
  const text = String(req.query.text || "សួស្តី! នេះគឺជាការសាកល្បងបំប្លែងអក្សរជាសំឡេង។ សូមអរគុណសម្រាប់ការប្រើប្រាស់សេវាកម្មនេះ។ សូមឱ្យអ្នកមានថ្ងៃល្អ។").slice(0, 500);
  const chunkMax = Number(req.query.chunk) || chunkLimit(voice);
  const t0 = Date.now();
  try {
    const { opus, chunks } = await synthesize(text, voice, "+0%", AbortSignal.timeout(TIMEOUT_MS), chunkMax);
    res.json({ ok: true, version: VERSION, voice, textLength: len(text), chunkMax, chunks, bytes: opus.length, ms: Date.now() - t0 });
  } catch (err) {
    res.status(502).json({ ok: false, version: VERSION, voice, textLength: len(text), chunkMax, ms: Date.now() - t0, error: err.message });
  }
});

// ---------- Probes: tell us exactly what Microsoft accepts from this server ----------
const SREY = "km-KH-SreymomNeural", PISETH = "km-KH-PisethNeural";
const LONG = "នេះគឺជាការសាកល្បងបំប្លែងអក្សរជាសំឡេងភាសាខ្មែរ។ សូមអរគុណសម្រាប់ការប្រើប្រាស់សេវាកម្មរបស់យើងខ្ញុំក្នុងថ្ងៃនេះ។ សូមឱ្យអ្នកមានថ្ងៃល្អ និងសុខភាពល្អជានិច្ច។ សូមអរគុណ។";
const part = (n) => cutByLength(LONG, n)[0].trim();
const SINGLE_PROBES = [
  ["khmer-short", SREY, "សួស្តី"],
  ["khmer-45", SREY, part(45)],
  ["khmer-60", SREY, part(60)],
  ["khmer-80", SREY, part(80)],
  ["khmer-100", SREY, part(100)],
  ["piseth-45", PISETH, part(45)],
  ["piseth-80", PISETH, part(80)],
];
let lastProbe = null;

async function probeOne(name, voice, text) {
  const t0 = Date.now();
  const session = new EdgeSession(voice);
  try {
    const mp3 = await session.speak(text, "+0%", AbortSignal.timeout(15_000));
    return { name, voice, chars: len(text), ok: true, audioSec: +(mp3.length / 6000).toFixed(1), ms: Date.now() - t0 };
  } catch (err) {
    return { name, voice, chars: len(text), ok: false, ms: Date.now() - t0, error: err.message.slice(0, 60) };
  } finally {
    session.close();
  }
}

// Several 35-char chunks: one socket reused sequentially, and 3 sockets at once
async function probeMulti(name, voice, parallel) {
  const t0 = Date.now();
  const chunks = splitText(LONG, 35).slice(0, parallel ? 3 : 4);
  const sessions = Array.from({ length: parallel ? 3 : 1 }, () => new EdgeSession(voice));
  const signal = AbortSignal.timeout(30_000);
  let okCount = 0;
  try {
    if (parallel) {
      const rs = await Promise.allSettled(chunks.map((c, i) => sessions[i].speak(c, "+0%", signal)));
      okCount = rs.filter((r) => r.status === "fulfilled").length;
    } else {
      for (const c of chunks) {
        try { await sessions[0].speak(c, "+0%", signal); okCount++; } catch {}
      }
    }
  } finally {
    sessions.forEach((x) => x.close());
  }
  return { name, voice, chars: chunks.map(len).join("+"), ok: okCount === chunks.length, okCount, of: chunks.length, ms: Date.now() - t0 };
}

async function runProbes() {
  const results = [];
  const log = (r) => console.log(
    `PROBE ${r.ok ? "ok  " : "FAIL"} ${r.name.padEnd(14)} ${r.voice} chars=${r.chars}` +
    (r.audioSec !== undefined ? ` audio=${r.audioSec}s` : "") +
    (r.of ? ` ok=${r.okCount}/${r.of}` : "") + ` ${r.ms}ms` + (r.error ? ` (${r.error})` : ""));
  const steps = [
    ...SINGLE_PROBES.map(([n, v, t]) => () => probeOne(n, v, t)),
    () => probeMulti("seq-4x35", SREY, false),
    () => probeMulti("par-3x35", SREY, true),
  ];
  for (const step of steps) {
    const r = await step();
    results.push(r);
    log(r);
    await sleep(1000); // do not hammer Edge while diagnosing it
  }
  lastProbe = { version: VERSION, at: new Date().toISOString(), results };
  return lastProbe;
}

app.get("/probe", limiter, async (req, res) => {
  if (!authorized(req)) return res.status(401).json({ error: "Unauthorized" });
  res.json(req.query.run === "1" || !lastProbe ? await runProbes() : lastProbe);
});

const server = app.listen(PORT, () => {
  console.log(`Server ${VERSION} running on port ${PORT}`);
  if (process.env.PROBE_ON_START !== "0") setTimeout(() => runProbes().catch((e) => console.error("probe error", e)), 3000);
});

for (const sig of ["SIGTERM", "SIGINT"]) {
  process.on(sig, () => {
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(1), 10_000).unref();
  });
}
