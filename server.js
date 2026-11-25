import express from "express";
import rateLimit from "express-rate-limit";
import { spawn } from "node:child_process";
import { createHash, timingSafeEqual } from "node:crypto";
import { MsEdgeTTS, OUTPUT_FORMAT } from "msedge-tts";

// ---------- Config (override via environment variables) ----------
const PORT = Number(process.env.PORT) || 3000;
const API_KEY = process.env.API_KEY || "";                    // empty = auth disabled
const MAX_TEXT = Number(process.env.MAX_TEXT) || 3000;        // characters per request
const MAX_CONCURRENT = Number(process.env.MAX_CONCURRENT) || 4;
const TIMEOUT_MS = Number(process.env.TIMEOUT_MS) || 30_000;
const CACHE_MAX = Number(process.env.CACHE_MAX) || 200;       // cached clips
const DEFAULT_VOICE = "km-KH-SreymomNeural";
const VOICE_RE = /^[a-z]{2,3}-[A-Z]{2}-\w+Neural$/;

const SOURCE_FORMAT = OUTPUT_FORMAT.AUDIO_24KHZ_48KBITRATE_MONO_MP3;
const FFMPEG_ARGS = [
  "-hide_banner", "-loglevel", "error",
  "-i", "pipe:0",
  "-vn",
  "-c:a", "libopus",
  "-application", "voip",   // tuned for speech
  "-b:a", "32k",            // plenty for mono speech, smaller files
  "-ar", "48000",
  "-ac", "1",
  "-f", "ogg",
  "pipe:1",
];

// ---------- Helpers ----------
const sha = (s) => createHash("sha256").update(s).digest();

function safeEqual(a, b) {
  return timingSafeEqual(sha(a), sha(b)); // equal-length digests, constant time
}

// "1.25x" -> "+25%", "0.75x" -> "-25%", clamped to 0.5x–2x
function parseRate(speed) {
  const n = Math.min(2, Math.max(0.5, parseFloat(speed) || 1));
  const pct = Math.round((n - 1) * 100);
  return `${pct >= 0 ? "+" : ""}${pct}%`;
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

async function synthesize(text, voice, rate, signal) {
  const tts = new MsEdgeTTS();
  let ffmpeg;
  try {
    await tts.setMetadata(voice, SOURCE_FORMAT);
    const { audioStream } = tts.toStream(text, { rate });

    return await new Promise((resolve, reject) => {
      if (signal.aborted) return reject(signal.reason);
      signal.addEventListener("abort", () => reject(signal.reason), { once: true });

      ffmpeg = spawn("ffmpeg", FFMPEG_ARGS, { stdio: ["pipe", "pipe", "pipe"] });

      const chunks = [];
      let stderr = "";
      ffmpeg.stdout.on("data", (c) => chunks.push(c));
      ffmpeg.stderr.on("data", (d) => { if (stderr.length < 2000) stderr += d; });
      ffmpeg.stdin.on("error", () => {}); // ignore EPIPE if ffmpeg dies early
      ffmpeg.on("error", reject);
      ffmpeg.on("close", (code) => {
        const out = Buffer.concat(chunks);
        code === 0 && out.length
          ? resolve(out)
          : reject(new Error(`ffmpeg exited ${code}: ${stderr.trim() || "no output"}`));
      });

      audioStream.on("error", reject);
      audioStream.pipe(ffmpeg.stdin);
    });
  } finally {
    try { tts.close(); } catch {}
    if (ffmpeg && ffmpeg.exitCode === null) ffmpeg.kill("SIGKILL");
  }
}

// ---------- App ----------
const app = express();
app.disable("x-powered-by");
app.set("trust proxy", 1); // correct client IPs behind Render/Railway/etc.
app.use(express.json({ limit: "32kb" }));

app.get("/", (_req, res) => res.send("Edge TTS + FFmpeg service is live."));
app.get("/health", (_req, res) => res.json({ ok: true, active, cached: cache.size }));

const limiter = rateLimit({ windowMs: 60_000, limit: 30, standardHeaders: true, legacyHeaders: false });

let active = 0;

app.post("/tts", limiter, async (req, res) => {
  if (API_KEY && !safeEqual(String(req.get("x-api-key") || ""), API_KEY)) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const { text, voice = DEFAULT_VOICE, speed = "1.0x" } = req.body ?? {};
  if (typeof text !== "string" || !text.trim()) {
    return res.status(400).json({ error: "Missing text" });
  }
  if (text.length > MAX_TEXT) {
    return res.status(413).json({ error: `Text exceeds ${MAX_TEXT} characters` });
  }
  if (typeof voice !== "string" || !VOICE_RE.test(voice)) {
    return res.status(400).json({ error: "Invalid voice" });
  }

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

  // Abort work on timeout or if the client disconnects before we finish
  const client = new AbortController();
  res.on("close", () => { if (!res.writableFinished) client.abort(new Error("client closed")); });
  const signal = AbortSignal.any([client.signal, AbortSignal.timeout(TIMEOUT_MS)]);

  active++;
  try {
    const audio = await synthesize(text, voice, rate, signal);
    cacheSet(key, audio);
    if (!res.destroyed) send(audio, false);
  } catch (err) {
    console.error("TTS error:", err.message);
    if (!res.headersSent && !res.destroyed) {
      res.status(err.name === "TimeoutError" ? 504 : 502).json({ error: "TTS failed" });
    }
  } finally {
    active--;
  }
});

const server = app.listen(PORT, () => console.log(`Server running on port ${PORT}`));

// Graceful shutdown for container platforms
for (const sig of ["SIGTERM", "SIGINT"]) {
  process.on(sig, () => {
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(1), 10_000).unref();
  });
}
