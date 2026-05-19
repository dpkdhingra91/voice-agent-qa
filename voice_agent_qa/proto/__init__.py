"""Vendored Pipecat protobuf frame definitions.

`frames.proto` and the pre-generated `frames_pb2.py` are copies from
upstream pipecat (`src/pipecat/frames/frames.proto` and
`src/pipecat/frames/protobufs/frames_pb2.py`). They are vendored here so
this package does NOT need to depend on the full `pipecat-ai` PyPI
package, which drags in `torch`, `transformers`, and `onnxruntime` and is
intended for server-side pipelines, not test clients.

Regenerate when upstream changes:

    pip install 'grpcio-tools~=1.67.1'
    python -m grpc_tools.protoc \\
        --proto_path=voice_agent_qa/proto \\
        --python_out=voice_agent_qa/proto \\
        frames.proto

Upstream license: BSD 2-Clause (preserved in frames.proto).
"""

from voice_agent_qa.proto import frames_pb2

__all__ = ["frames_pb2"]
