import express from "express";
import rateLimit from "express-rate-limit";
import { spawn } from "node:child_process";
import { createHash, timingSafeEqual } from "node:crypto";
import { MsEdgeTTS, OUTPUT_FORMAT } from "msedge-tts";

// ---------- Config (override via environment variables) ----------
const VERSION = "1.4.0-adaptive";
const PORT = Number(process.env.PORT) || 3000;
const API_KEY = process.env.API_KEY || "";                    // empty = auth disabled
const MAX_TEXT = Number(process.env.MAX_TEXT) || 2000;        // characters per request
const MAX_CONCURRENT = Number(process.env.MAX_CONCURRENT) || 4;
const TIMEOUT_MS = Number(process.env.TIMEOUT_MS) || 60_000;
const CACHE_MAX = Number(process.env.CACHE_MAX) || 200;       // cached clips
const CHUNK_MAX = Number(process.env.CHUNK_MAX) || 40;        // starting max characters per Edge request
const MIN_CHUNK = 10;                                         // never split below this many characters
const CHUNK_PARALLEL = Number(process.env.CHUNK_PARALLEL) || 3;
const DEBUG = process.env.DEBUG_TTS === "1";                  // verbose msedge-tts protocol logging
const DEFAULT_VOICE = process.env.DEFAULT_VOICE || "km-KH-SreymomNeural";
const VOICE_RE = /^[a-z]{2,3}-[A-Z]{2}-\w+Neural$/;

const SOURCE_FORMAT = OUTPUT_FORMAT.AUDIO_24KHZ_48KBITRATE_MONO_MP3;
const FFMPEG_ARGS = [
  "-hide_banner", "-loglevel", "error",
  "-i", "pipe:0",
  "-vn",
  "-c:a", "libopus",
  "-application", "voip",   // tuned for speech
  "-b:a", "32k",
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
  const worker = async () => {
    while (!failed && !signal.aborted && next < items.length) {
      const i = next++;
      try { results[i] = await fn(items[i], i); } catch (e) { failed = true; throw e; }
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  if (signal.aborted) throw signal.reason;
  return results;
}

// One Edge TTS request -> MP3 buffer
function collect(stream, signal) {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(signal.reason);
    const chunks = [];
    signal.addEventListener("abort", () => reject(signal.reason), { once: true });
    stream.on("data", (c) => chunks.push(c));
    stream.on("error", reject);
    stream.on("end", () => {
      const buf = Buffer.concat(chunks);
      buf.length ? resolve(buf) : reject(new Error("No audio data received"));
    });
  });
}

async function edgeMp3(text, voice, rate, signal) {
  const tts = new MsEdgeTTS({ enableLogger: DEBUG, synthesisTimeoutMs: 20_000 });
  try {
    await tts.setMetadata(voice, SOURCE_FORMAT);
    const { audioStream } = tts.toStream(toSsmlText(text), { rate });
    return await collect(audioStream, signal);
  } finally {
    try { tts.close(); } catch {}
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

// Per-voice learned chunk size: shrinks when Edge rejects a chunk that works when halved
const voiceLimit = new Map();
const chunkLimit = (voice) => voiceLimit.get(voice) ?? CHUNK_MAX;

// Edge sometimes closes the socket on a chunk. Instead of retrying the same text,
// split it in half and speak each half (recursively), then join the audio.
async function speakChunk(text, voice, rate, signal) {
  try {
    return await edgeMp3(text, voice, rate, signal);
  } catch (err) {
    if (signal.aborted) throw err;
    const parts = len(text) > MIN_CHUNK ? halve(text) : null;
    if (!parts) {
      console.warn(`Chunk failed (${err.message}) len=${len(text)} text=${JSON.stringify(text)}; retrying as-is`);
      await sleep(300);
      return edgeMp3(text, voice, rate, signal);
    }
    console.warn(`Chunk failed len=${len(text)} text=${JSON.stringify(text.slice(0, 40))}; splitting into ${len(parts[0])}+${len(parts[1])}`);
    const bufs = [];
    for (const part of parts) bufs.push(await speakChunk(part, voice, rate, signal));
    const smaller = Math.max(MIN_CHUNK, Math.floor(len(text) * 0.6));
    if (smaller < chunkLimit(voice)) {
      voiceLimit.set(voice, smaller);
      console.warn(`Chunk limit for ${voice} lowered to ${smaller}`);
    }
    return Buffer.concat(bufs);
  }
}

// MP3 buffer -> Ogg/Opus buffer
function toOpus(mp3, signal) {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(signal.reason);
    const ff = spawn("ffmpeg", FFMPEG_ARGS, { stdio: ["pipe", "pipe", "pipe"] });
    const out = [];
    let stderr = "";
    signal.addEventListener("abort", () => { ff.kill("SIGKILL"); reject(signal.reason); }, { once: true });
    ff.stdout.on("data", (c) => out.push(c));
    ff.stderr.on("data", (d) => { if (stderr.length < 2000) stderr += d; });
    ff.stdin.on("error", () => {});
    ff.on("error", reject);
    ff.on("close", (code) => {
      const buf = Buffer.concat(out);
      code === 0 && buf.length
        ? resolve(buf)
        : reject(new Error(`ffmpeg exited ${code}: ${stderr.trim() || "no output"}`));
    });
    ff.stdin.end(mp3);
  });
}

async function synthesize(text, voice, rate, signal, chunkMax = chunkLimit(voice)) {
  const chunks = splitText(text, chunkMax);
  if (!chunks.length) throw Object.assign(new Error("Nothing speakable in text"), { status: 400 });
  const mp3s = await mapLimit(chunks, CHUNK_PARALLEL, (c) => speakChunk(c, voice, rate, signal), signal);
  const opus = await toOpus(Buffer.concat(mp3s), signal);
  return { opus, chunks: chunks.length };
}

// ---------- App ----------
const app = express();
app.disable("x-powered-by");
app.set("trust proxy", 1);
app.use(express.json({ limit: "32kb" }));

app.get("/", (_req, res) => res.send("Edge TTS + FFmpeg service is live."));
app.get("/health", (_req, res) => res.json({ ok: true, version: VERSION, active, cached: cache.size }));

const limiter = rateLimit({ windowMs: 60_000, limit: 30, standardHeaders: true, legacyHeaders: false });

let active = 0;

app.post("/tts", limiter, async (req, res) => {
  if (!authorized(req)) return res.status(401).json({ error: "Unauthorized" });

  const { text, voice = DEFAULT_VOICE, speed = "1.0x" } = req.body ?? {};
  if (typeof text !== "string" || !text.trim()) return res.status(400).json({ error: "Missing text" });
  if (text.length > MAX_TEXT) return res.status(413).json({ error: `Text exceeds ${MAX_TEXT} characters` });
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
  const signal = AbortSignal.any([client.signal, AbortSignal.timeout(TIMEOUT_MS)]);

  active++;
  try {
    const { opus } = await synthesize(text, voice, rate, signal);
    cacheSet(key, opus);
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
const K45 = "នេះគឺជាការសាកល្បងបំប្លែងអក្សរជាសំឡេងភាសាខ្មែរ";
const PROBES = [
  ["khmer-short", SREY, "សួស្តី"],
  ["khmer-45", SREY, K45],
  ["khmer-100", SREY, `${K45}។ សូមអរគុណសម្រាប់ការប្រើប្រាស់សេវាកម្មរបស់យើងខ្ញុំក្នុងថ្ងៃនេះ។`],
  ["khmer+latin", SREY, "ប្រើ Temp Mail Bot ភ្លាមៗ"],
  ["latin-only", SREY, "Temp Mail Bot"],
  ["khmer-digits", SREY, "ជាង ១០ ឆ្នាំ"],
  ["piseth-khmer-45", PISETH, K45],
  ["piseth-khmer+latin", PISETH, "ប្រើ Temp Mail Bot ភ្លាមៗ"],
  ["english-voice", "en-US-AriaNeural", "Temp Mail Bot"],
];
let lastProbe = null;

async function runProbes() {
  const results = [];
  for (const [name, voice, text] of PROBES) {
    const t0 = Date.now();
    try {
      const mp3 = await edgeMp3(text, voice, "+0%", AbortSignal.timeout(15_000));
      results.push({ name, voice, chars: len(text), ok: true, bytes: mp3.length, ms: Date.now() - t0 });
    } catch (err) {
      results.push({ name, voice, chars: len(text), ok: false, ms: Date.now() - t0, error: err.message.slice(0, 80) });
    }
    const r = results.at(-1);
    console.log(`PROBE ${r.ok ? "ok  " : "FAIL"} ${name.padEnd(20)} ${voice} chars=${r.chars} ${r.ms}ms${r.ok ? ` bytes=${r.bytes}` : ` (${r.error})`}`);
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
