#!/usr/bin/env python3
"""
tests/mock_adversarial_proc.py
Adversarial subprocess for stress-testing HeadlessAgentAdapter.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import sys
import os
import json
import time

def main():
    mode = os.environ.get("ADVERSARIAL_MODE", "normal")
    conv_id = os.environ.get("MOCK_CONV_ID", "adv-conv-1")

    # Handshake init event
    init_payload = {
        "event": "init",
        "conversation_id": conv_id,
        "init": {
            "cwd": os.getcwd(),
            "tools": ["herdr_pane_run", "run_command"],
        }
    }
    sys.stdout.write(json.dumps(init_payload) + "\n")
    sys.stdout.flush()

    turn_count = 0
    while True:
        line = sys.stdin.readline()
        if not line:
            break

        turn_count += 1
        try:
            req = json.loads(line)
        except Exception:
            req = {}

        prompt = req.get("message", {}).get("content", "")

        if mode == "kernel_pipe_unconsumed_leak" and turn_count == 1:
            # Emit 1 delta then flood 800 unconsumed lines into the OS pipe without result
            delta = {
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 1,
                    "text_delta": "Turn 1 first delta",
                }
            }
            sys.stdout.write(json.dumps(delta) + "\n")
            for i in range(800):
                stale_line = {
                    "event": "step_update",
                    "step_update": {
                        "step_type": "agent_response",
                        "state": "ACTIVE",
                        "step_index": 2 + i,
                        "text_delta": f"STALE_UNCONSUMED_LEAK_{i}",
                    }
                }
                sys.stdout.write(json.dumps(stale_line) + "\n")
            sys.stdout.flush()
            # Do NOT send result for turn 1

        elif mode == "kernel_pipe_unconsumed_leak" and turn_count > 1:
            # Turn 2: send legitimate response
            delta = {
                "event": "step_update",
                "step_update": {
                    "step_type": "agent_response",
                    "state": "ACTIVE",
                    "step_index": 1,
                    "text_delta": f"Authentic: {prompt}",
                }
            }
            sys.stdout.write(json.dumps(delta) + "\n")
            result = {
                "event": "result",
                "result": {
                    "status": "SUCCESS",
                    "response": f"Authentic: {prompt}",
                }
            }
            sys.stdout.write(json.dumps(result) + "\n")
            sys.stdout.flush()

        elif mode == "unbounded_flood_leak":
            # Output huge volume of lines
            for i in range(5000):
                line_data = {
                    "event": "step_update",
                    "step_update": {
                        "step_type": "agent_response",
                        "state": "ACTIVE",
                        "step_index": i,
                        "text_delta": f"FLOOD_{i}",
                    }
                }
                sys.stdout.write(json.dumps(line_data) + "\n")
            sys.stdout.flush()

        else:
            # Default normal echo
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
            result = {
                "event": "result",
                "result": {
                    "status": "SUCCESS",
                    "response": f"Echo: {prompt}",
                }
            }
            sys.stdout.write(json.dumps(result) + "\n")
            sys.stdout.flush()

if __name__ == "__main__":
    main()
