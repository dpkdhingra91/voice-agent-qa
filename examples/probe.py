"""Minimal connection probe.

Opens a Pipecat WS, registers callbacks that print every event, waits for
the bot to finish, prints a summary. No audio is sent — the bot will run
its opening turn and then sit waiting for mic input that never comes.

Usage:

    export PIPECAT_BASE_URL=https://pipecat.example.com
    export PIPECAT_MEETING_ID=<your-test-meeting-uuid>
    python examples/probe.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time

from voice_agent_qa import PipecatClient


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
)
logger = logging.getLogger("probe")


async def main() -> int:
    base_url = os.environ.get("PIPECAT_BASE_URL")
    meeting_id = os.environ.get("PIPECAT_MEETING_ID")

    if not base_url or not meeting_id:
        print(
            "Set PIPECAT_BASE_URL (e.g. https://pipecat.example.com) and "
            "PIPECAT_MEETING_ID (a server-side test session id).",
            file=sys.stderr,
        )
        return 2

    # Optional app-specific connect params. Adapt to your server's /connect
    # contract. The AIIA reference server expects these fields:
    extra = {
        "position": os.environ.get("POSITION", "Backend Engineer"),
        "candidate_name": os.environ.get("CANDIDATE_NAME", "QA Probe"),
        "interview_type": os.environ.get("INTERVIEW_TYPE", "Screening"),
        "interview_mode": "candidate",
        "language_code": os.environ.get("LANGUAGE_CODE", "en"),
        "interview_time": int(os.environ.get("INTERVIEW_TIME", "120")),
    }

    client = PipecatClient(base_url=base_url)

    started_at = time.time()
    bot_turns = 0
    user_turns = 0

    async def on_bot_transcript(text: str) -> None:
        nonlocal bot_turns
        bot_turns += 1
        elapsed = time.time() - started_at
        print(f"[+{elapsed:6.1f}s] BOT  ▶ {text}")

    async def on_user_transcript(text: str, final: bool) -> None:
        nonlocal user_turns
        if final:
            user_turns += 1
            elapsed = time.time() - started_at
            print(f"[+{elapsed:6.1f}s] USER ▶ {text}")

    async def on_turn_enabled() -> None:
        print("[gate] user turn enabled")
        # If you wanted to drive the conversation, you'd send audio here.
        # The probe just observes and lets the session time out naturally.

    async def on_turn_disabled() -> None:
        print("[gate] user turn disabled (bot speaking)")

    def on_error(data: dict) -> None:
        print(f"[error] {data}")

    client.register_bot_transcript_callback(on_bot_transcript)
    client.register_user_transcript_callback(on_user_transcript)
    client.register_turn_enabled_callback(on_turn_enabled)
    client.register_turn_disabled_callback(on_turn_disabled)
    client.register_error_callback(on_error)

    print(f"Connecting to {base_url} (meeting={meeting_id})…")
    await client.connect(meeting_id=meeting_id, extra_params=extra)
    print("Connected. Waiting for bot turns. Ctrl-C to bail.")

    try:
        # Wait either for the WS to close from the server side (session
        # timeout) or for a max wall-clock of 5 minutes — whichever first.
        await asyncio.wait_for(client.wait_closed(), timeout=300)
    except asyncio.TimeoutError:
        print("Probe wall-clock timeout — disconnecting.")
        await client.disconnect()
        await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Interrupted — disconnecting.")
        await client.disconnect()

    elapsed = time.time() - started_at
    print(
        f"\nDone. {bot_turns} bot turns, {user_turns} final user transcripts "
        f"in {elapsed:.1f}s."
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
