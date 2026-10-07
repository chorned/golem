"""Herdr -> WebUI hand discovery, driven by a real `herdr pane list` capture.

Run from the repo root:  python3 -m unittest golem.webui.tests.test_hand_discovery
"""

import asyncio
import json
import os
import stat
import tempfile
import unittest

import aiohttp

from golem.webui.hand_discovery import hand_fields, is_hand_pane, parse_pane_list
from golem.webui.server import WebUIServer

PORT = 8793

# Verbatim (trimmed of nothing relevant) from `herdr pane list` on the hermes host.
REAL_PANE_LIST = {"id": "cli:pane:list", "result": {"type": "pane_list", "panes": [
    {"agent_status": "unknown", "cwd": "/Users/hermes", "label": "oci-worker", "pane_id": "w3:p1", "terminal_title": "opc@claw-primary-nic:~"},
    {"agent_status": "unknown", "cwd": "/Users/hermes", "label": "laptop-worker", "pane_id": "w4:p1"},
    {"agent_status": "unknown", "cwd": "/Users/hermes", "label": "pc-worker", "pane_id": "w5:p1"},
    {"agent": "agy", "agent_session": {"agent": "agy", "kind": "id", "value": "88633cda"}, "agent_status": "idle",
     "cwd": "/Users/hermes", "label": "chief-of-staff", "pane_id": "w6:p1"},
    {"agent_session": {"agent": "hermes", "kind": "id", "value": "20260926_143215_a13cad"}, "agent_status": "unknown",
     "cwd": "/Users/hermes", "label": "hermes-worker", "pane_id": "w7:p1"},
    {"agent_status": "unknown", "cwd": "/Users/hermes/projects/AGY", "foreground_cwd": "/Users/hermes/projects/AGY", "pane_id": "w8:p1"},
    {"agent_status": "unknown", "cwd": "/Users/hermes/.herdr/worktrees/AGY/task-1791313295-hermes",
     "foreground_cwd": "/Users/hermes/.herdr/worktrees/AGY/task-1791313295-hermes", "pane_id": "w29:p1"},
    {"agent_status": "unknown", "cwd": "/Users/hermes/projects/AGY", "pane_id": "w2A:p1"},
    {"agent_status": "unknown", "cwd": "/Users/hermes/projects/AGY", "pane_id": "w2B:p1"},
]}}

FAKE_HERDR = """#!/bin/sh
case "$1 $2" in
  "pane list") cat "$FAKE_PANES" ;;
  "pane read") printf 'live output of %s\\n' "$3" ;;
  *) exit 2 ;;
esac
"""


class PureLogicTests(unittest.TestCase):
    def test_parse_and_classify_real_capture(self):
        panes = parse_pane_list(json.dumps(REAL_PANE_LIST))
        self.assertEqual(len(panes), 9)
        hide = {"oci-worker", "laptop-worker", "pc-worker", "chief-of-staff"}
        hands = [p["pane_id"] for p in panes if is_hand_pane(p, hide)]
        # hermes-worker (agent session) and the task worktree pane; not ssh panes, the liaison, or plain shells.
        self.assertEqual(hands, ["w7:p1", "w29:p1"])

    def test_unlabelled_pane_falls_back_to_pane_id_and_status_maps(self):
        panes = {p["pane_id"]: p for p in parse_pane_list(json.dumps(REAL_PANE_LIST))}
        f = hand_fields(panes["w29:p1"])
        self.assertEqual(f["title"], "w29:p1")
        self.assertEqual(f["status"], "RUNNING")  # herdr 'unknown' -> neutral running
        self.assertEqual(hand_fields(panes["w6:p1"])["status"], "IDLE")

    def test_visible_when_not_hidden(self):
        panes = {p["pane_id"]: p for p in parse_pane_list(json.dumps(REAL_PANE_LIST))}
        self.assertTrue(is_hand_pane(panes["w6:p1"], set()))


class DiscoveryIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.panes_file = os.path.join(self.tmp.name, "panes.json")
        self._write(REAL_PANE_LIST)
        herdr = os.path.join(self.tmp.name, "herdr")
        with open(herdr, "w") as fh:
            fh.write(FAKE_HERDR)
        os.chmod(herdr, os.stat(herdr).st_mode | stat.S_IEXEC)
        self._env = {k: os.environ.get(k) for k in (
            "GOLEM_HERDR_BIN", "FAKE_PANES", "GOLEM_WEBUI_HERDR_SYNC_INTERVAL", "GOLEM_WEBUI_HERDR_SYNC")}
        os.environ.update({
            "GOLEM_HERDR_BIN": herdr, "FAKE_PANES": self.panes_file,
            "GOLEM_WEBUI_HERDR_SYNC_INTERVAL": "0.15", "GOLEM_WEBUI_HERDR_SYNC": "1"})
        self.srv = WebUIServer(host="127.0.0.1", port=PORT)
        await self.srv.start()
        self.base = f"http://127.0.0.1:{PORT}"

    async def asyncTearDown(self):
        await self.srv.stop()
        for k, v in self._env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        self.tmp.cleanup()

    def _write(self, doc):
        with open(self.panes_file, "w") as fh:
            json.dump(doc, fh)

    async def _hands(self):
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{self.base}/api/status") as r:
                return (await r.json())["hands"]

    async def _wait_for(self, pred, timeout=3.0):
        end = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < end:
            hands = await self._hands()
            if pred(hands):
                return hands
            await asyncio.sleep(0.1)
        self.fail(f"condition not met; hands={[h['pane_id'] for h in await self._hands()]}")

    async def test_unregistered_herdr_agents_appear_in_the_webui(self):
        hands = await self._wait_for(lambda h: len(h) == 2)
        self.assertEqual({h["pane_id"] for h in hands}, {"hermes-worker", "w29:p1"})

    async def test_discovered_hands_are_mirrored_by_real_pane_id(self):
        await self._wait_for(lambda h: len(h) == 2)
        self.assertEqual(set(self.srv._pane_targets.values()), {"w7:p1", "w29:p1"})

    async def test_new_agent_appears_and_finished_one_disappears(self):
        await self._wait_for(lambda h: len(h) == 2)
        doc = json.loads(json.dumps(REAL_PANE_LIST))
        doc["result"]["panes"] = [p for p in doc["result"]["panes"] if p["pane_id"] != "w7:p1"]
        doc["result"]["panes"].append({"agent": "agy", "agent_status": "working", "cwd": "/Users/hermes",
                                       "label": "agy-task-2", "pane_id": "w40:p1"})
        self._write(doc)
        hands = await self._wait_for(lambda h: {x["pane_id"] for x in h} == {"w29:p1", "agy-task-2"})
        working = next(h for h in hands if h["pane_id"] == "agy-task-2")
        self.assertEqual(working["status"], "RUNNING")

    async def test_status_change_is_reflected(self):
        await self._wait_for(lambda h: len(h) == 2)
        doc = json.loads(json.dumps(REAL_PANE_LIST))
        for p in doc["result"]["panes"]:
            if p["pane_id"] == "w7:p1":
                p["agent_status"] = "blocked"
        self._write(doc)
        await self._wait_for(lambda h: any(x["pane_id"] == "hermes-worker" and x["status"] == "BLOCKED" for x in h))

    async def test_runner_registered_hands_coexist_with_discovered_ones(self):
        self.srv.register_active_hand("runner", "localhost", "podcast check", 31824, mirror=False)
        hands = await self._wait_for(lambda h: len(h) == 3)
        self.assertEqual({h["pane_id"] for h in hands}, {"runner", "hermes-worker", "w29:p1"})

    async def test_discovery_does_not_duplicate_a_hand_the_runner_already_registered(self):
        self.srv.register_active_hand("hermes-worker", "localhost", "explicit", 1, herdr_pane_id="w7:p1", mirror=False)
        hands = await self._wait_for(lambda h: any(x["pane_id"] == "w29:p1" for x in h))
        self.assertEqual(sorted(h["pane_id"] for h in hands), ["hermes-worker", "w29:p1"])
        self.assertEqual(next(h for h in hands if h["pane_id"] == "hermes-worker")["command"], "explicit")

    async def test_pane_frames_flow_for_discovered_hands(self):
        await self._wait_for(lambda h: len(h) == 2)
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{self.base}/events") as resp:
                async def find():
                    event = None
                    async for raw in resp.content:
                        line = raw.decode().rstrip("\n")
                        if line.startswith("event: "):
                            event = line[7:]
                        elif line.startswith("data: ") and event == "pane_frame":
                            d = json.loads(line[6:])
                            if "live output of w29:p1" in d["text"]:
                                return d["pane_id"]
                self.assertEqual(await asyncio.wait_for(find(), 4), "w29:p1")


if __name__ == "__main__":
    unittest.main()
