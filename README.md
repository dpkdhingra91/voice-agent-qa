# voice-agent-qa

Drive a [Pipecat](https://github.com/pipecat-ai/pipecat) voice agent as a bot, from Python. For nightly smoke tests, regression checks, latency measurement, and headless reproduction of issues that only show up end-to-end.

There is **no `pipecat-ai-client` package on PyPI**. The browser uses `@pipecat-ai/client-js`. The upstream `pipecat-ai` package is server-side and drags in `torch` + `transformers` + `onnxruntime`. This library implements the Pipecat WebSocket protocol directly — small, fast, no ML dependencies.

## What's in the box

- `PipecatClient` — does the two-step `POST /connect → WSS handshake → RTVI v1.3.0` dance.
- Vendored protobuf definitions (BSD 2-Clause, from upstream Pipecat).
- Async callbacks for the event surface that matters: bot transcript, user transcript (interim + final), bot-started/stopped speaking, server-messages, turn-gate enabled/disabled, errors.
- `send_audio_pcm()` for shipping mic audio (16 kHz mono 16-bit LE).
- A connection probe example you can run today.

## Install

```bash
pip install voice-agent-qa
```

## Quickstart — connect and log every event

```python
import asyncio
from voice_agent_qa import PipecatClient

async def main():
    client = PipecatClient(base_url="https://pipecat.example.com")
    client.register_bot_transcript_callback(lambda t: print(f"[BOT] {t}"))
    client.register_user_transcript_callback(
        lambda t, final: print(f"[USER {'final' if final else 'interim'}] {t}")
    )

    await client.connect(
        meeting_id="3f4ff883-...",
        extra_params={
            "position": "Backend Engineer",
            "candidate_name": "QA Probe",
            "language_code": "en",
            # ... whatever your server's /connect expects
        },
    )

    # Wait for the session to close on its own, or do something interactive.
    await client.wait_closed()

asyncio.run(main())
```

Or just run the included probe:

```bash
export PIPECAT_BASE_URL=https://pipecat.example.com
export PIPECAT_MEETING_ID=<your-test-meeting-uuid>
python examples/probe.py
```

## The protocol — what we discovered so you don't have to

Spent a couple weeks reverse-engineering this. The good news: it's small and clean. The bad news: it's nowhere documented for Python.

### 1. Two-step handshake

```
POST https://<host>/connect
body: { meeting_id: ..., ...whatever your server needs }
→ { "ws_url": "/ws?sid=<12-hex>" }

WS  wss://<host><ws_url>
```

The `sid` is a one-shot redemption coupon — the server pops it from its sessions dict the moment the WS connects. Open within ~10 seconds.

### 2. Wire format

Protobuf-framed binary, not JSON. Every WS message is a `pipecat.frames.frames.Frame`:

```proto
message Frame {
  oneof frame {
    TextFrame          text          = 1;
    AudioRawFrame      audio         = 2;   // 16 kHz in, 24 kHz out, mono, 16-bit LE
    TranscriptionFrame transcription = 3;
    MessageFrame       message       = 4;   // JSON RTVI envelope inside
  }
}
```

The `frames.proto` file is tiny — vendored in `voice_agent_qa/proto/frames.proto`.

### 3. RTVI envelopes ride inside `MessageFrame.data`

Every "event" the server emits is JSON wrapped in an RTVI envelope:

```json
{ "label": "rtvi-ai", "type": "bot-transcription", "id": "abc123", "data": {"text": "..."} }
```

Server → client event types we handle:
- `bot-transcription` — full assistant turn text
- `user-transcription` — STT result, interim or final
- `bot-started-speaking` / `bot-stopped-speaking`
- `bot-ready` — protocol handshake complete
- `server-message` — app-specific server-side events
- `error` — `{"error": "...", "fatal": bool}`

Client → server:
- `client-ready` — sent automatically after WS opens
- `client-message` — your app-level messages
- `disconnect-bot` — politely end the server pipeline

### 4. Audio

- **Mic → server:** 16 kHz mono PCM, 16-bit LE, ~512 samples per frame (1024 bytes). Use `send_audio_pcm(chunk)`.
- **Bot → client:** 24 kHz mono PCM, no WAV header. You receive these in the `Frame.audio` branch. The default client logs at DEBUG; override `_dispatch` to route the audio.

## Server `/connect` body — your call

The `extra_params` dict is merged into the POST body verbatim. Whatever your Pipecat server's `/connect` route reads, put there. The most common shape (used by the AIIA reference implementation) is:

```python
extra_params = {
    "position": "Backend Engineer",
    "candidate_name": "QA Probe",
    "interview_type": "Screening",
    "interview_mode": "candidate",
    "language_code": "en",
    "interview_time": 120,
}
```

If your server uses different field names, just change them — the client doesn't care.

## What this is NOT

- **Not a TTS bot.** The probe receives the bot's voice but doesn't speak back. Drive `send_audio_pcm()` from your own TTS (Azure / ElevenLabs / Sarvam / piper) to make a complete talking bot. The original AIIA QA harness uses Azure Speech TTS — see [origin notes](#origin) below.
- **Not a server-side framework.** Pipecat itself is the server. This client connects to one.
- **Not WebRTC.** This is the WebSocket transport. If your Pipecat server uses Daily WebRTC, this library doesn't apply (yet).

## Audio out — receiving and saving

The default `_dispatch` logs audio frames at DEBUG and drops them. To capture the bot's voice, subclass:

```python
import wave
from voice_agent_qa import PipecatClient

class RecordingClient(PipecatClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._out = wave.open("bot-output.wav", "wb")
        self._out.setnchannels(1)
        self._out.setsampwidth(2)
        self._out.setframerate(24000)

    async def _dispatch(self, proto):
        if proto.WhichOneof("frame") == "audio":
            self._out.writeframes(proto.audio.audio)
            return
        await super()._dispatch(proto)
```

## Compatibility

- Python ≥ 3.10
- Pipecat server RTVI v1.3.0 (other versions may work; the envelope shape is stable)
- protobuf 5.x or 6.x

## Origin

Extracted from the QA harness for [AIIA](https://aiinterviewagents.com) — a production voice-interview pipeline. The harness runs nightly against prod (Sarvam STT + Azure OpenAI + Sarvam TTS) to catch regressions before users do.

The interview-flow assertions, persona LLM, and TTS-driven candidate bot are *not* in this open-source extraction — they're too domain-specific. What's here is the reusable kernel: the protocol implementation. PRs welcome.

## License

MIT — see [LICENSE](LICENSE).

`voice_agent_qa/proto/frames.proto` is BSD 2-Clause from upstream Pipecat (preserved in the file header).
