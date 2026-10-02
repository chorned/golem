#!/usr/bin/env python3
"""
tests/mock_agy_proc.py
Deterministic mock CLI binary simulating agy stream-json protocol for subprocess testing.
"""

import sys
import os
import json
import time

def main():
    mode = os.environ.get("MOCK_AGY_MODE", "normal")
    conv_id = os.environ.get("MOCK_AGY_CONV_ID", "conv-mock-1")

    # Send init event
    init_payload = {
        "event": "init",
        "conversation_id": conv_id,
        "init": {
            "cwd": os.getcwd(),
            "tools": ["herdr_pane_run", "herdr_agent_read"],
        }
    }
    sys.stdout.write(json.dumps(init_payload) + "\n")
    sys.stdout.flush()

    if mode == "init_only_exit":
        sys.exit(0)

    # Turn loop
    while True:
        line = sys.stdin.readline()
        if not line:
            break

        try:
            req = json.loads(line)
        except Exception:
            req = {}

        prompt = req.get("message", {}).get("content", "")

        if mode == "crash_after_first_delta":
            # Output one delta then sleep forever until killed
            delta = {
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 1,
                    "text_delta": "Processing started before crash...",
                }
            }
            sys.stdout.write(json.dumps(delta) + "\n")
            sys.stdout.flush()
            while True:
                time.sleep(1.0)

        elif mode == "exit_nonzero_mid_turn":
            delta = {
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 1,
                    "text_delta": "Dying abruptly with exit code 42...",
                }
            }
            sys.stdout.write(json.dumps(delta) + "\n")
            sys.stdout.flush()
            sys.stderr.write("Fatal error: simulated crash in mock agy\n")
            sys.stderr.flush()
            sys.exit(42)

        else:
            # Normal streaming turn
            delta = {
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 1,
                    "text_delta": f"Echo: {prompt}",
                }
            }
            sys.stdout.write(json.dumps(delta) + "\n")
            sys.stdout.flush()

            result = {
                "event": "result",
                "result": {
                    "status": "SUCCESS",
                    "response": f"Echo: {prompt}",
                    "duration_seconds": 0.05,
                    "usage": {"input_tokens": 10, "output_tokens": 10},
                }
            }
            sys.stdout.write(json.dumps(result) + "\n")
            sys.stdout.flush()

if __name__ == "__main__":
    main()
