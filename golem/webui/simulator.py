#!/usr/bin/env python3
"""
golem/webui/simulator.py
Lightweight Hand Agent Simulator for testing, demonstrations, and WebUI verification.
Simulates N concurrent workers across the Herdr fleet mesh with realistic commands,
PIDs, statuses, and live streaming ANSI terminal deltas to xterm.js.

Minimally bloats the project:
- Zero external dependencies (uses standard library asyncio, random, time).
- Self-contained, clean lifecycle: start(), stop(), and per-tick ANSI stream generation.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import time
import random
import asyncio
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger("Golem.WebUI.Simulator")

PRESET_SIMULATED_HANDS: List[Dict[str, Any]] = [
    {
        "pane_id": "w1:p2",
        "node": "localhost",
        "worker": "agy",
        "command": "cargo test --workspace --no-fail-fast",
        "pid": 31820,
        "type": "cargo_test",
    },
    {
        "pane_id": "w4:p1",
        "node": "personal-laptop",
        "worker": "laptop-worker",
        "command": "python3 -m unittest discover tests -p 'test_sensory*.py'",
        "pid": 48211,
        "type": "python_test",
    },
    {
        "pane_id": "w5:p1",
        "node": "desktop-pc",
        "worker": "pc-worker",
        "command": "python3 train_lora.py --batch-size 16 --cuda",
        "pid": 9104,
        "type": "pytorch_train",
    },
    {
        "pane_id": "w3:p1",
        "node": "oci-machine",
        "worker": "oci-worker",
        "command": "tail -f /var/log/nginx/access.log | grep 200",
        "pid": 7721,
        "type": "nginx_logs",
    },
    {
        "pane_id": "w7:p1",
        "node": "localhost",
        "worker": "hermes-worker",
        "command": "docker compose up -d postgres redis",
        "pid": 14299,
        "type": "docker_compose",
    },
    {
        "pane_id": "w9:p1",
        "node": "localhost",
        "worker": "opencode",
        "command": "agy -p 'refactor sensory observer' --verify",
        "pid": 63102,
        "type": "code_refactor",
    },
    {
        "pane_id": "w12:p1",
        "node": "localhost",
        "worker": "agy",
        "command": "npm run build && vite optimize",
        "pid": 28410,
        "type": "npm_build",
    },
    {
        "pane_id": "w15:p1",
        "node": "localhost",
        "worker": "hermes-worker",
        "command": "ffmpeg -hide_banner -nostdin -i carlflix.mkv -c copy stream.mp4",
        "pid": 51928,
        "type": "media_encode",
    },
    {
        "pane_id": "w18:p1",
        "node": "oci-machine",
        "worker": "oci-worker",
        "command": "alembic upgrade head",
        "pid": 82019,
        "type": "db_migration",
    },
]
SIMULATED_WORKER_PRESETS = PRESET_SIMULATED_HANDS


class HandSimulator:
    """
    Lightweight Hand Agent Simulator for testing multiple concurrent hand cards in WebUI.
    Can be dynamically started/stopped with any hand count 1..9.
    """

    def __init__(self, server: Any, count: int = 6) -> None:
        self.server = server
        self.count = max(1, min(count, len(PRESET_SIMULATED_HANDS)))
        self._task: Optional[asyncio.Task[Any]] = None
        self._running: bool = False
        self._active_panes: List[Dict[str, Any]] = []
        self._step_counters: Dict[str, int] = {}
        self._buffers: Dict[str, List[str]] = {}

    @property
    def is_running(self) -> bool:
        return self._running and self._task is not None and not self._task.done()

    def start(self) -> None:
        """Register hands and start background streaming task."""
        if self._running:
            return
        self._running = True
        self._active_panes = list(PRESET_SIMULATED_HANDS[:self.count])
        now_str = time.strftime("%H:%M:%S")

        for spec in self._active_panes:
            pid = spec["pid"]
            pane = spec["pane_id"]
            node = spec["node"]
            cmd = spec["command"]
            worker = spec["worker"]

            self._step_counters[pane] = 0
            initial_line = f"\x1b[1;36m[GOLEM SIMULATOR]\x1b[0m Worker attached on {node} ({pane})\r\n$ {cmd}\r\n"
            self._buffers[pane] = [initial_line]

            # Register hand authoritative in WebUIServer
            self.server.register_active_hand(
                pane_id=pane,
                node=node,
                command=cmd,
                pid=pid,
                worker=worker,
                mirror=False,
                herdr_pane_id=pane,
            )

            # Send spawn frame
            try:
                self.server.broadcast("hand_spawn", {
                    "pane_id": pane,
                    "node": node,
                    "command": cmd,
                    "pid": pid,
                    "worker": worker,
                    "status": "RUNNING",
                    "buffer_tail": initial_line,
                })
                self.server.broadcast("pane_frame", {
                    "pane_id": pane,
                    "text": initial_line,
                })
            except Exception as e:
                logger.debug(f"HandSimulator spawn frame: {e}")

        logger.info(f"HandSimulator started {len(self._active_panes)} simulated hand agents.")
        try:
            loop = asyncio.get_running_loop()
            self._task = loop.create_task(self._simulation_loop())
        except RuntimeError:
            pass

    async def _simulation_loop(self) -> None:
        """Periodic loop that streams realistic ANSI output deltas to active hand cards."""
        try:
            while self._running:
                await asyncio.sleep(random.uniform(1.2, 2.4))
                if not self._running:
                    break

                # Pick 1 to 3 random workers to produce output this tick
                sample_count = min(len(self._active_panes), random.randint(1, 3))
                chosen = random.sample(self._active_panes, sample_count)

                for spec in chosen:
                    pane = spec["pane_id"]
                    step = self._step_counters.get(pane, 0) + 1
                    self._step_counters[pane] = step
                    delta = self._generate_delta(spec, step)
                    if not delta:
                        continue

                    buf = self._buffers.setdefault(pane, [])
                    buf.append(delta)
                    if len(buf) > 50:
                        del buf[:-50]

                    # Push delta to xterm.js via SSE
                    self.server.broadcast("pane_frame", {
                        "pane_id": pane,
                        "text": delta,
                        "timestamp": time.time(),
                    })
                    self.server.update_hand_buffer(pane, "".join(buf[-10:]), spec["node"])
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in HandSimulator loop: {e}", exc_info=True)

    def _generate_delta(self, spec: Dict[str, Any], step: int) -> str:
        """Generate realistic ANSI terminal output strings based on worker type."""
        wtype = spec.get("type", "generic")
        now_ts = time.strftime("%H:%M:%S")

        if wtype == "cargo_test":
            tests = ["sensory::eyes::test_oauth_blocker", "gateway::ingress::test_yield", "webui::server::test_hands_broadcast", "execution::harness::test_dual_pane"]
            tname = tests[step % len(tests)]
            dur = f"{random.uniform(0.01, 0.12):.3f}s"
            return f"test {tname} ... \x1b[32mok\x1b[0m ({dur})\r\n"

        elif wtype == "python_test":
            dots = "." * random.randint(2, 6)
            passed = 10 + (step * 5)
            return f"\x1b[32m{dots}\x1b[0m [Ran {passed} tests in {step * 0.4:.1f}s — \x1b[1;32mOK\x1b[0m]\r\n"

        elif wtype == "pytorch_train":
            epoch = (step // 10) + 1
            cur_step = (step * 25) % 1000
            loss = max(0.08, 1.45 - (step * 0.04) + random.uniform(-0.02, 0.02))
            vram = f"{12.4 + random.uniform(0.1, 0.8):.1f}GB"
            return f"\x1b[35m[Epoch {epoch}/50]\x1b[0m step \x1b[33m{cur_step:04d}/1000\x1b[0m - loss: \x1b[32m{loss:.4f}\x1b[0m - vram: {vram} - lr: 1e-4\r\n"

        elif wtype == "nginx_logs":
            ip = f"100.80.{random.randint(10, 99)}.{random.randint(2, 254)}"
            rt = f"{random.uniform(0.8, 3.2):.1f}ms"
            return f"[{now_ts}] \x1b[32m200 OK\x1b[0m GET /api/status from {ip} in {rt}\r\n"

        elif wtype == "docker_compose":
            states = ["Running (healthy)", "Container redis Up 4h", "Syncing WAL checkpoints (nominal)"]
            msg = states[step % len(states)]
            return f"\x1b[34m[compose]\x1b[0m postgres-1 | {now_ts} [LOG] {msg}\r\n"

        elif wtype == "code_refactor":
            actions = ["Inspecting AST nodes...", "Applying replacement chunk...", "Running unit verification...", "Git diff clean; 0 regressions."]
            msg = actions[step % len(actions)]
            return f"\x1b[36m[opencode]\x1b[0m {msg}\r\n"

        elif wtype == "npm_build":
            modules = step * 14
            return f"\x1b[32m✓\x1b[0m built in {step * 180}ms ({modules} modules transformed)\r\n"

        elif wtype == "media_encode":
            frame = step * 48
            fps = random.randint(58, 62)
            return f"frame={frame:5d} fps={fps} q=22.0 size={step * 420}kB time=00:0{step % 60}:12 bitrate=2410kbits/s speed=2.1x\r"

        elif wtype == "db_migration":
            return f"\x1b[33mINFO\x1b[0m  [alembic.runtime.migration] Running upgrade head -> rev_{step:04d}_add_indices\r\n"

        return f"[{now_ts}] {spec['node']} task heartbeat: step {step}\r\n"

    def stop(self) -> None:
        """Stop background simulation and unregister all simulated hands."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            self._task = None

        for spec in self._active_panes:
            pane = spec["pane_id"]
            node = spec["node"]
            try:
                self.server.unregister_active_hand(pane, node)
                self.server.broadcast("hand_settle", {"pane_id": pane, "node": node})
            except Exception as e:
                logger.debug(f"HandSimulator stop unregister: {e}")

        self._active_panes.clear()
        self._step_counters.clear()
        self._buffers.clear()
        try:
            self.server.broadcast("hands_status", list(self.server.active_hands.values()))
        except Exception:
            pass
        logger.info("HandSimulator stopped; simulated hands settled cleanly.")
