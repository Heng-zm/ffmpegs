import express from "express";
    import { spawn } from "child_process";
    import { MsEdgeTTS, OUTPUT_FORMAT } from "msedge-tts";

    const app = express();
    app.use(express.json());

    // Healthcheck endpoint for Render
    app.get("/", (req, res) => res.send("Edge TTS + FFmpeg service is running."));

    app.post("/tts", async (req, res) => {
      const { text, voice = "km-KH-SreymomNeural", speed = "1.0x" } = req.body;
      if (!text) return res.status(400).json({ error: "Missing text" });

      try {
        const tts = new MsEdgeTTS();
        // Use Microsoft Edge standard audio stream
        await tts.setMetadata(voice, OUTPUT_FORMAT.AUDIO_24KHZ_48KBITRATE_MONO_MP3);

        const readable = tts.toStream(text, {
          rate: speed === "1.25x" ? "+25%" : speed === "1.5x" ? "+50%" : speed === "0.75x" ? "-25%" : "+0%"
        });

        // Spawn FFmpeg to convert MP3 stream into native Telegram Ogg Opus (mono, 48kHz, 48kbps)
        const ffmpeg = spawn("ffmpeg", [
          "-i", "pipe:0",       // Read MP3 from stdin
          "-c:a", "libopus",    // Encode with Opus codec
          "-b:a", "48k",        // 48kbps bitrate
          "-ar", "48000",       // 48kHz sample rate
          "-ac", "1",           // Mono (1 channel)
          "-f", "ogg",          // Ogg container
          "pipe:1"              // Output to stdout
        ]);

        res.setHeader("Content-Type", "audio/ogg");
        res.setHeader("Access-Control-Allow-Origin", "*");

        readable.pipe(ffmpeg.stdin);
        ffmpeg.stdout.pipe(res);

        ffmpeg.stderr.on("data", () => {}); // silence ffmpeg logs
        ffmpeg.on("error", (err) => {
          console.error("FFmpeg error:", err);
          if (!res.headersSent) res.status(500).end();
        });
      } catch (err) {
        console.error("TTS error:", err);
        if (!res.headersSent) res.status(500).json({ error: err.message });
      }
    });

    const PORT = process.env.PORT || 3000;
    app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
