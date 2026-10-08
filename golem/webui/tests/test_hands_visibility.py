"""Hands registry / WebUI visibility guarantees: every registered hand must reach the browser.

Run from the repo root:  python3 -m unittest golem.webui.tests.test_hands_visibility

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import asyncio
import json
import threading
import unittest

import aiohttp

from golem.webui.server import WebUIServer

PORT = 8792


async def _read_events(resp, want, timeout):
    """Consume SSE until ``want(event, payload)`` returns non-None; raise on timeout."""
    event, data = None, []

    async def _loop():
        nonlocal event
        async for raw in resp.content:
            line = raw.decode().rstrip("\n")
            if line.startswith("event: "):
                event = line[7:]
            elif line.startswith("data: "):
                data.append(line[6:])
            elif line == "" and data:
                payload = "\n".join(data)
                data.clear()
                got = want(event, payload)
                if got is not None:
                    return got

    return await asyncio.wait_for(_loop(), timeout)


def _connected(ev, _):
    return True if ev == "connected" else None


class HandsVisibilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.srv = WebUIServer(host="127.0.0.1", port=PORT)
        await self.srv.start()
        self.base = f"http://127.0.0.1:{PORT}"

    async def asyncTearDown(self):
        await self.srv.stop()

    async def _status_hands(self):
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{self.base}/api/status") as r:
                return (await r.json())["hands"]

    async def test_nine_distinct_hands_all_reported(self):
        for i in range(9):
            self.srv.register_active_hand(f"pane-{i}", "localhost", f"cmd {i}", 100 + i, mirror=False)
        hands = await self._status_hands()
        self.assertEqual(sorted(h["pane_id"] for h in hands), sorted(f"pane-{i}" for i in range(9)))

    async def test_same_pane_label_on_two_nodes_does_not_overwrite(self):
        self.srv.register_active_hand("runner", "localhost", "task A", 1, mirror=False)
        self.srv.register_active_hand("runner", "laptop-worker", "task B", 2, mirror=False)
        hands = await self._status_hands()
        self.assertEqual(len(hands), 2)
        self.assertEqual({h["command"] for h in hands}, {"task A", "task B"})
        self.assertEqual(len({h["pane_id"] for h in hands}), 2)

    async def test_nine_runner_labels_across_nodes(self):
        nodes = [f"node-{i}" for i in range(9)]
        for n in nodes:
            self.srv.register_active_hand("runner", n, f"job on {n}", None, mirror=False)
        hands = await self._status_hands()
        self.assertEqual(len(hands), 9)
        self.assertEqual({h["node"] for h in hands}, set(nodes))

    async def test_same_node_same_label_distinct_herdr_panes(self):
        self.srv.register_active_hand("runner", "localhost", "A", 1, herdr_pane_id="p-1", mirror=False)
        self.srv.register_active_hand("runner", "localhost", "B", 2, herdr_pane_id="p-2", mirror=False)
        self.assertEqual(len(await self._status_hands()), 2)

    async def test_reregistering_same_hand_replaces_not_duplicates(self):
        self.srv.register_active_hand("runner", "localhost", "A", 1, mirror=False)
        self.srv.register_active_hand("runner", "localhost", "A2", 1, mirror=False)
        hands = await self._status_hands()
        self.assertEqual(len(hands), 1)
        self.assertEqual(hands[0]["command"], "A2")

    async def test_buffer_updates_and_unregister_are_node_aware(self):
        self.srv.register_active_hand("runner", "localhost", "A", 1, mirror=False)
        self.srv.register_active_hand("runner", "laptop-worker", "B", 2, mirror=False)
        self.srv.update_hand_buffer("runner", "ONLY-LAPTOP\n", node="laptop-worker")
        hands = {h["node"]: h for h in await self._status_hands()}
        self.assertIn("ONLY-LAPTOP", hands["laptop-worker"]["buffer_tail"])
        self.assertNotIn("ONLY-LAPTOP", hands["localhost"]["buffer_tail"])
        self.srv.unregister_active_hand("runner", node="laptop-worker")
        self.assertEqual([h["node"] for h in await self._status_hands()], ["localhost"])

    async def test_registration_is_pushed_immediately_not_on_heartbeat(self):
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{self.base}/events") as resp:
                await _read_events(resp, _connected, 3)
                self.srv.register_active_hand("a", "localhost", "x", 1, mirror=False)
                self.srv.register_active_hand("b", "laptop-worker", "y", 2, mirror=False)

                def two_hands(ev, payload):
                    return True if ev == "hands_status" and len(json.loads(payload)) == 2 else None

                # The heartbeat fires every 2.5s; this must land well inside that window.
                self.assertTrue(await _read_events(resp, two_hands, 1.0))

    async def test_registration_from_worker_threads_is_thread_safe(self):
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{self.base}/events") as resp:
                await _read_events(resp, _connected, 3)

                def worker(i):
                    self.srv.register_active_hand(f"t-{i}", "localhost", f"c{i}", i, mirror=False)

                threads = [threading.Thread(target=worker, args=(i,)) for i in range(9)]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()

                def nine(ev, payload):
                    return True if ev == "hands_status" and len(json.loads(payload)) == 9 else None

                self.assertTrue(await _read_events(resp, nine, 2.0))

    async def test_finishing_one_hand_does_not_clear_the_others(self):
        for i in range(3):
            self.srv.register_active_hand(f"p{i}", "localhost", "c", i, mirror=False)
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{self.base}/events") as resp:
                await _read_events(resp, _connected, 3)

                def initial(ev, payload):
                    return True if ev == "hands_status" and len(json.loads(payload)) == 3 else None

                # The server primes every new connection with the current hands; consume that first.
                self.assertTrue(await _read_events(resp, initial, 1.0))
                self.srv.unregister_active_hand("p1")

                def after(ev, payload):
                    if ev == "hands_status":
                        return sorted(h["pane_id"] for h in json.loads(payload))
                    return None

                self.assertEqual(await _read_events(resp, after, 1.0), ["p0", "p2"])

    async def test_kill_hand_api_endpoint_unregisters_and_broadcasts(self):
        self.srv.register_active_hand("worker-1", "localhost", "long task", 1234, mirror=False)
        self.assertIn("worker-1", self.srv._active_hands)

        async with aiohttp.ClientSession() as s:
            async with s.post(f"{self.base}/api/hands/worker-1/kill") as resp:
                self.assertEqual(resp.status, 200)
                data = await resp.json()
                self.assertTrue(data.get("ok"))
                self.assertEqual(data.get("pane_id"), "worker-1")

        self.assertNotIn("worker-1", self.srv._active_hands)
        hands = await self._status_hands()
        self.assertEqual(len(hands), 0)

    async def test_kill_hand_delete_method(self):
        self.srv.register_active_hand("worker-delete", "localhost", "rm task", 5678, mirror=False)
        self.assertIn("worker-delete", self.srv._active_hands)

        async with aiohttp.ClientSession() as s:
            async with s.delete(f"{self.base}/api/hands/worker-delete") as resp:
                self.assertEqual(resp.status, 200)
                data = await resp.json()
                self.assertTrue(data.get("ok"))

        self.assertNotIn("worker-delete", self.srv._active_hands)


if __name__ == "__main__":
    unittest.main()
