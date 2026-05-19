"""voice-agent-qa — drive a Pipecat voice agent as a bot, from Python.

Useful for:
- Nightly smoke tests against your production voice pipeline
- Regression / latency measurement
- CI gating on voice-flow behavior
- Headless reproduction of issues that only manifest end-to-end

No `pipecat-ai-client` package exists on PyPI — the JS frontend uses
`@pipecat-ai/client-js`, and the upstream `pipecat-ai` package pulls in
torch + transformers for server-side pipelines. This library implements
the Pipecat WebSocket protocol (protobuf-framed `Frame` messages + RTVI
v1.3.0 envelopes) directly, with no heavy deps.

See README and `examples/probe.py` for usage.
"""

from .client import ConnectParams, PipecatClient, RTVI_PROTOCOL_VERSION

__all__ = ["ConnectParams", "PipecatClient", "RTVI_PROTOCOL_VERSION"]
__version__ = "0.1.0"
