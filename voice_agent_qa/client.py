"""PipecatClient — RTVI client over a raw protobuf-framed WebSocket.

============================================================================
PIPECAT WS PROTOCOL — protocol notes
============================================================================

Pipecat is a server-side voice-agent framework. Its WebSocket transport
speaks a small custom protocol (NOT plain JSON) — this module is the
client side of that protocol, written from scratch so it doesn't pull in
the server-side framework's heavy ML deps.

------------------------------------------------------------------------
1. Handshake
------------------------------------------------------------------------

The browser (and us) does a two-step handshake:

    1a. POST https://<your-pipecat-host>/connect
        body: {                       # the exact field shape is app-specific.
          "meeting_id": "<uuid>",     # ConnectParams below carries the AIIA
          ...                         # original shape; pass extra_params={} if
        }                             # your server uses different field names.
        response: { "ws_url": "/ws?sid=<12-hex-id>" }

    1b. Open `wss://<your-pipecat-host><ws_url>`

The sid is a one-shot redemption coupon — the server pops it from its
in-memory sessions dict the moment the WS connects.

------------------------------------------------------------------------
2. Wire format
------------------------------------------------------------------------

The WS uses Pipecat's **Protobuf frame serializer** (NOT plain JSON):

    serializer=ProtobufFrameSerializer()
    audio_in_enabled=True,   audio_in_sample_rate=16000
    audio_out_enabled=True,  audio_out_sample_rate=24000
    add_wav_header=False

Each binary message is a serialized `pipecat.frames.frames.Frame` protobuf.
The `.proto` is tiny — 4 message types wrapped in a oneof:

    Frame.text          TextFrame{id, name, text}
    Frame.audio         AudioRawFrame{id, name, audio, sample_rate, num_channels}
    Frame.transcription TranscriptionFrame{id, name, text, user_id, timestamp}
    Frame.message       MessageFrame{data}                   # JSON string

All RTVI events — bot transcripts, user transcripts, server-messages,
client-messages — travel as MessageFrame.data containing a JSON-serialized
RTVI envelope:

    { "label": "rtvi-ai",
      "type":  "<event-type>",
      "id":    "<message-uuid>",
      "data":  {...} }

See `voice_agent_qa/proto/frames.proto` (vendored from upstream Pipecat,
BSD 2-Clause) for the protobuf definitions.

------------------------------------------------------------------------
3. Event types (RTVI v1.3.0)
------------------------------------------------------------------------

Server → client (JSON envelope `{"label":"rtvi-ai","type":"<T>","data":{...}}`):

    "bot-transcription"        data={"text":"..."}                  # full assistant turn
    "user-transcription"       data={"text":"...","user_id":"","timestamp":"","final":bool}
    "bot-started-speaking"     (no data)
    "bot-stopped-speaking"     (no data)
    "bot-ready"                data={"version":"1.3.0","about":{...}}
    "server-message"           data={"type":"<app-specific>", ...}
    "error"                    data={"error":"...","fatal":bool}

Client → server:

    "client-ready"             data={"version":"1.3.0","about":{"library":"voice-agent-qa"}}
    "client-message"           data={"t":"<type>","d":{...}}
    "disconnect-bot"           (no data — triggers EndTaskFrame in server pipeline)

The JS frontend re-flattens these for app code: `client.on("botTranscript",
data)` is the RTVI envelope's `data` field for a `bot-transcription` event.
The Python event callbacks below follow the same shape.

------------------------------------------------------------------------
4. Audio frame shape
------------------------------------------------------------------------

Client mic audio (us → server) is 16 kHz mono PCM, 16-bit little-endian,
chunked into frames of 512 samples (~32 ms each). Send via `send_audio_pcm()`
below — each call serializes one `AudioRawFrame` message and ships it. The
server expects the chunks to be already padded/truncated to a fixed size by
the caller; we don't enforce a chunker here.

Bot output (TTS audio coming back) is 24 kHz mono PCM, no WAV header.
We receive these as `Frame.audio` in `_recv_loop` and currently log only —
override `_dispatch` in a subclass to route the audio somewhere useful.

============================================================================
End protocol notes.
============================================================================
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

import httpx
import websockets
from websockets.asyncio.client import ClientConnection

from voice_agent_qa.proto import frames_pb2

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────
# Connect handshake — POST /connect → relative ws_url
# ─────────────────────────────────────────────────────────────────────────


@dataclass
class ConnectParams:
    """Body of POST /connect.

    Apps differ on which fields they require. The `extra_params` dict is
    merged into the JSON body — put whatever your server expects there
    (e.g. `meeting_id`, `position`, `language_code`, etc.). The dataclass
    fields below are the most common shape (carried over from the original
    AIIA reference implementation) — set them or override with extra_params.
    """

    meeting_id: str = ""
    extra_params: dict = field(default_factory=dict)

    def to_body(self) -> dict:
        body: dict = {}
        if self.meeting_id:
            body["meeting_id"] = self.meeting_id
        body.update(self.extra_params)
        return body


# ─────────────────────────────────────────────────────────────────────────
# RTVI envelope helpers
# ─────────────────────────────────────────────────────────────────────────


RTVI_LABEL = "rtvi-ai"
RTVI_PROTOCOL_VERSION = "1.3.0"


def _rtvi_envelope(msg_type: str, data: Any = None, msg_id: Optional[str] = None) -> dict:
    """Build the standard RTVI message envelope (matches RTVI.Message)."""
    env: dict[str, Any] = {
        "label": RTVI_LABEL,
        "type": msg_type,
        "id": msg_id or uuid.uuid4().hex[:12],
    }
    if data is not None:
        env["data"] = data
    return env


# ─────────────────────────────────────────────────────────────────────────
# Callback type aliases
# ─────────────────────────────────────────────────────────────────────────


# All callbacks accept either coroutine functions or plain callables.
# `_maybe_await` handles both.
BotTranscriptCallback = Callable[[str], Any]                       # (text)
UserTranscriptCallback = Callable[[str, bool], Any]                # (text, final)
SimpleCallback = Callable[[], Any]                                 # ()
ServerMessageCallback = Callable[[str, dict], Any]                 # (type, data)
ErrorCallback = Callable[[dict], Any]                              # (data)


async def _maybe_await(value: Any) -> None:
    if asyncio.iscoroutine(value):
        await value


# ─────────────────────────────────────────────────────────────────────────
# PipecatClient — RTVI client over a raw protobuf-framed WebSocket
# ─────────────────────────────────────────────────────────────────────────


class PipecatClient:
    """A minimal Pipecat RTVI client.

    Wraps the two-step /connect → WS handshake, the protobuf framing, and
    the RTVI v1.3.0 message envelope. Event callbacks mirror the JS
    `@pipecat-ai/client-js` surface the real frontend uses.

    Example:

        client = PipecatClient(
            base_url="https://pipecat.example.com",
            ws_base="wss://pipecat.example.com",
        )
        client.register_bot_transcript_callback(lambda t: print(f"[BOT] {t}"))
        client.register_turn_enabled_callback(lambda: print("[GATE] enabled"))

        await client.connect(extra_params={"meeting_id": "...", "position": "..."})
        await client.send_client_message("client:mic_ready")
        # ... drive interaction (send PCM, react to events) ...
        await client.disconnect()
    """

    # 16-bit LE PCM @ 16 kHz mono → 512 samples = 1024 bytes per frame
    AUDIO_IN_SAMPLE_RATE = 16000
    AUDIO_IN_CHANNELS = 1
    AUDIO_OUT_SAMPLE_RATE = 24000

    def __init__(self, base_url: str, ws_base: Optional[str] = None):
        """Args:
        base_url: HTTPS URL to POST /connect against, e.g. https://pipecat.example.com
        ws_base:  WSS base for the WebSocket (defaults to base_url with https→wss).
        """
        self.base_url = base_url.rstrip("/")
        if ws_base is None:
            ws_base = self.base_url.replace("https://", "wss://").replace("http://", "ws://")
        self.ws_base = ws_base.rstrip("/")

        self._ws: Optional[ClientConnection] = None
        self._recv_task: Optional[asyncio.Task] = None
        self._connected = asyncio.Event()
        self._closed = asyncio.Event()

        # Event callbacks
        self._on_bot_transcript: Optional[BotTranscriptCallback] = None
        self._on_user_transcript: Optional[UserTranscriptCallback] = None
        self._on_bot_started_speaking: Optional[SimpleCallback] = None
        self._on_bot_stopped_speaking: Optional[SimpleCallback] = None
        self._on_server_message: Optional[ServerMessageCallback] = None
        self._on_turn_enabled: Optional[SimpleCallback] = None
        self._on_turn_disabled: Optional[SimpleCallback] = None
        self._on_error: Optional[ErrorCallback] = None

    # ---- public lifecycle ----

    async def connect(
        self,
        meeting_id: str = "",
        extra_params: Optional[dict] = None,
        *,
        client_library: str = "voice-agent-qa",
        client_library_version: str = "0.1.0",
    ) -> None:
        """Run the /connect handshake, open the WS, announce client-ready.

        `extra_params` is merged into the POST /connect body — put whatever
        fields your server expects there (e.g. `position`, `language_code`,
        `interview_type`, etc.).
        """
        if self._ws is not None:
            raise RuntimeError("PipecatClient already connected")

        params = ConnectParams(
            meeting_id=meeting_id,
            extra_params=extra_params or {},
        )
        body = params.to_body()

        logger.info("POST %s/connect body_keys=%s", self.base_url, list(body.keys()))
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.post(f"{self.base_url}/connect", json=body)
            r.raise_for_status()
            data = r.json()

        ws_path = data.get("ws_url")
        if not ws_path:
            raise RuntimeError(f"/connect did not return ws_url: {data}")
        ws_url = (
            ws_path
            if ws_path.startswith(("ws://", "wss://"))
            else f"{self.ws_base}{ws_path}"
        )
        logger.info("ws_url=%s", ws_url)

        self._ws = await websockets.connect(ws_url, max_size=None)
        self._connected.set()
        self._closed.clear()

        # Spawn the receive loop FIRST so we can observe `bot-ready` etc.
        self._recv_task = asyncio.create_task(
            self._recv_loop(), name="pipecat-recv-loop"
        )

        # Announce client-ready — the server's RTVIProcessor uses this to
        # trigger the initial pipeline run.
        await self._send_message(
            _rtvi_envelope(
                "client-ready",
                data={
                    "version": RTVI_PROTOCOL_VERSION,
                    "about": {
                        "library": client_library,
                        "library_version": client_library_version,
                        "platform": "python",
                    },
                },
            )
        )

    async def disconnect(self) -> None:
        """Send `disconnect-bot` then close the WS."""
        if self._ws is None:
            return
        try:
            await self._send_message(_rtvi_envelope("disconnect-bot"))
        except Exception:  # noqa: BLE001
            logger.debug("disconnect-bot send failed (already closed?)", exc_info=True)
        try:
            await self._ws.close()
        except Exception:  # noqa: BLE001
            logger.debug("ws.close raised", exc_info=True)
        self._ws = None
        self._closed.set()

        if self._recv_task is not None:
            try:
                await asyncio.wait_for(self._recv_task, timeout=2.0)
            except asyncio.TimeoutError:
                self._recv_task.cancel()
            self._recv_task = None

    @property
    def closed(self) -> asyncio.Event:
        """Set when the receive loop has exited (WS closed by either side)."""
        return self._closed

    async def wait_closed(self) -> None:
        await self._closed.wait()

    # ---- callbacks ----

    def register_bot_transcript_callback(self, callback: BotTranscriptCallback) -> None:
        self._on_bot_transcript = callback

    def register_user_transcript_callback(self, callback: UserTranscriptCallback) -> None:
        self._on_user_transcript = callback

    def register_bot_started_speaking_callback(self, callback: SimpleCallback) -> None:
        self._on_bot_started_speaking = callback

    def register_bot_stopped_speaking_callback(self, callback: SimpleCallback) -> None:
        self._on_bot_stopped_speaking = callback

    def register_server_message_callback(self, callback: ServerMessageCallback) -> None:
        self._on_server_message = callback

    def register_turn_enabled_callback(self, callback: SimpleCallback) -> None:
        self._on_turn_enabled = callback

    def register_turn_disabled_callback(self, callback: SimpleCallback) -> None:
        self._on_turn_disabled = callback

    def register_error_callback(self, callback: ErrorCallback) -> None:
        """Receive RTVI `error` envelopes (`{"error": "...", "fatal": bool}`).

        Useful for diagnosis when the server-side pipeline emits an ErrorFrame
        (LLM 5xx, content-filter strike, STT/TTS failure).
        """
        self._on_error = callback

    # ---- send helpers ----

    async def send_client_message(
        self, msg_type: str, data: Optional[dict] = None
    ) -> None:
        """Wrap `msg_type` / `data` in the RTVI `client-message` envelope and ship it.

        Matches `client.sendClientMessage(type, data)` in the JS SDK.
        """
        if self._ws is None:
            raise RuntimeError("PipecatClient is not connected")
        await self._send_message(
            _rtvi_envelope(
                "client-message",
                data={"t": msg_type, "d": data or {}},
            )
        )

    async def send_audio_pcm(self, pcm_bytes: bytes) -> None:
        """Send one frame of mic audio (16 kHz mono 16-bit LE).

        The caller is responsible for chunking — Pipecat expects fixed-size
        frames (512 samples = 1024 bytes each). Anything else still parses
        but the server may regroup it.
        """
        if self._ws is None:
            raise RuntimeError("PipecatClient is not connected")
        proto = frames_pb2.Frame()
        proto.audio.audio = pcm_bytes
        proto.audio.sample_rate = self.AUDIO_IN_SAMPLE_RATE
        proto.audio.num_channels = self.AUDIO_IN_CHANNELS
        await self._ws.send(proto.SerializeToString())

    # ---- internal ----

    async def _send_message(self, envelope: dict) -> None:
        """Wrap an RTVI envelope dict in a protobuf MessageFrame and send."""
        if self._ws is None:
            raise RuntimeError("PipecatClient is not connected")
        proto = frames_pb2.Frame()
        proto.message.data = json.dumps(envelope)
        await self._ws.send(proto.SerializeToString())
        logger.debug("→ %s id=%s", envelope.get("type"), envelope.get("id"))

    async def _recv_loop(self) -> None:
        """Drain the WS, parse protobuf Frames, dispatch RTVI envelopes."""
        assert self._ws is not None
        try:
            async for raw in self._ws:
                if isinstance(raw, str):
                    # Pipecat WS transport is binary-only; a stray string
                    # likely means a server error response in JSON.
                    logger.warning("unexpected text frame: %r", raw[:200])
                    continue
                try:
                    proto = frames_pb2.Frame.FromString(raw)
                except Exception:  # noqa: BLE001
                    logger.exception("protobuf decode failed (%d bytes)", len(raw))
                    continue
                await self._dispatch(proto)
        except websockets.exceptions.ConnectionClosed as exc:
            logger.info(
                "WS closed: code=%s reason=%s", exc.code, exc.reason or ""
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("recv loop crashed")
        finally:
            self._closed.set()

    async def _dispatch(self, proto) -> None:
        """Route a single decoded Frame to the right callback."""
        which = proto.WhichOneof("frame")

        if which == "message":
            try:
                envelope = json.loads(proto.message.data)
            except Exception:  # noqa: BLE001
                logger.exception("RTVI envelope JSON parse failed")
                return
            await self._dispatch_rtvi(envelope)
        elif which == "audio":
            logger.debug(
                "← audio frame: %d bytes, sr=%d ch=%d",
                len(proto.audio.audio),
                proto.audio.sample_rate,
                proto.audio.num_channels,
            )
        elif which == "transcription":
            logger.debug(
                "← TranscriptionFrame: user_id=%s text=%r",
                proto.transcription.user_id,
                proto.transcription.text,
            )
        elif which == "text":
            logger.debug("← TextFrame: %r", proto.text.text)
        else:
            logger.debug("← unknown frame oneof=%s", which)

    async def _dispatch_rtvi(self, envelope: dict) -> None:
        """Route a parsed RTVI envelope to the registered callback."""
        msg_type = envelope.get("type")
        data = envelope.get("data") or {}
        logger.debug("← %s data=%s", msg_type, _truncate(data))

        if msg_type == "bot-transcription":
            text = data.get("text", "")
            if text and self._on_bot_transcript is not None:
                await _maybe_await(self._on_bot_transcript(text))
        elif msg_type == "user-transcription":
            text = data.get("text", "")
            final = bool(data.get("final", False))
            if self._on_user_transcript is not None:
                await _maybe_await(self._on_user_transcript(text, final))
        elif msg_type == "bot-started-speaking":
            if self._on_bot_started_speaking is not None:
                await _maybe_await(self._on_bot_started_speaking())
        elif msg_type == "bot-stopped-speaking":
            if self._on_bot_stopped_speaking is not None:
                await _maybe_await(self._on_bot_stopped_speaking())
        elif msg_type == "server-message":
            inner_type = (data or {}).get("type") if isinstance(data, dict) else None
            if self._on_server_message is not None:
                await _maybe_await(
                    self._on_server_message(inner_type or "", data if isinstance(data, dict) else {})
                )
            if inner_type == "bot:user_turn_enabled" and self._on_turn_enabled is not None:
                await _maybe_await(self._on_turn_enabled())
            elif inner_type == "bot:user_turn_disabled" and self._on_turn_disabled is not None:
                await _maybe_await(self._on_turn_disabled())
        elif msg_type == "bot-ready":
            logger.info("bot-ready: %s", _truncate(data))
        elif msg_type == "error":
            logger.error("server error: %s", data)
            if self._on_error is not None:
                await _maybe_await(self._on_error(data if isinstance(data, dict) else {"error": str(data)}))
        else:
            # Lots of fine-grained events (bot-llm-started/stopped, metrics,
            # user-audio-level...) — debug only so they don't drown info logs.
            logger.debug("unhandled RTVI event: type=%s", msg_type)


def _truncate(obj: Any, n: int = 200) -> str:
    s = repr(obj)
    return s if len(s) <= n else s[:n] + "…"


__all__ = [
    "ConnectParams",
    "PipecatClient",
    "RTVI_PROTOCOL_VERSION",
]
