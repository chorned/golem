"""Quota-aware model selection, Linear-as-memory helpers, read-only hop cap, and idle-prompt annotation.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""
import asyncio
import unittest
from unittest import mock

from golem.drivers.antigravity import (
    AntigravityDriver,
    clear_exhausted_models,
    is_model_exhausted,
    mark_model_exhausted,
    parse_quota_reset_seconds,
)
from golem.ingress import (
    IngressGateway,
    READ_ONLY_HOP_TOOLS,
    _THREAD_REF,
    annotate_idle_shell_prompt,
    build_linear_notes_block,
    candidate_is_empty,
    compose_hand_brief,
    describe_candidate_shape,
    get_mouth_tools_declaration,
    strip_read_only_tools,
)
from golem.linear_bridge import LinearBridge


QUOTA_MSG = (
    "RESOURCE_EXHAUSTED (code 429): Individual quota reached. Please upgrade your subscription "
    "to increase your limits. Resets in 30h51m12s."
)


class QuotaRegistryTests(unittest.TestCase):
    def test_parses_reset_countdown(self):
        self.assertEqual(parse_quota_reset_seconds(QUOTA_MSG), 30 * 3600 + 51 * 60 + 12)
        self.assertEqual(parse_quota_reset_seconds("quota hit, resets in 7h"), 7 * 3600)
        self.assertEqual(parse_quota_reset_seconds("Resets in 39s"), 39)
        self.assertIsNone(parse_quota_reset_seconds("HTTP 429 Too Many Requests"))
        self.assertIsNone(parse_quota_reset_seconds(None))

    def test_exhaustion_expires(self):
        mark_model_exhausted("m1", "Resets in 10s")
        self.assertTrue(is_model_exhausted("m1"))
        with mock.patch("golem.drivers.antigravity.time.time", return_value=10**12):
            self.assertFalse(is_model_exhausted("m1"))
        # expired entries are dropped, not just ignored
        self.assertFalse(is_model_exhausted("m1"))

    def test_default_ttl_when_no_countdown(self):
        mark_model_exhausted("m2", "rate limit exceeded")
        self.assertTrue(is_model_exhausted("m2"))
        clear_exhausted_models()
        self.assertFalse(is_model_exhausted("m2"))

    def test_none_model_is_never_exhausted(self):
        mark_model_exhausted(None, QUOTA_MSG)
        self.assertFalse(is_model_exhausted(None))


class DriverSpawnSelectionTests(unittest.IsolatedAsyncioTestCase):
    def test_new_driver_skips_exhausted_default(self):
        mark_model_exhausted("gemini-default", QUOTA_MSG)
        drv = AntigravityDriver(cwd="/tmp", model="gemini-default", fallback_models=["sonnet", "other"])
        self.assertEqual(drv.model, "sonnet")
        self.assertEqual(drv.fallback_models, ["other"])

    def test_healthy_default_is_untouched(self):
        mark_model_exhausted("sonnet", QUOTA_MSG)
        drv = AntigravityDriver(cwd="/tmp", model="gemini-default", fallback_models=["sonnet"])
        self.assertEqual(drv.model, "gemini-default")

    def test_all_exhausted_keeps_configured_default(self):
        mark_model_exhausted("a", QUOTA_MSG)
        mark_model_exhausted("b", QUOTA_MSG)
        drv = AntigravityDriver(cwd="/tmp", model="a", fallback_models=["b"])
        self.assertEqual(drv.model, "a")

    async def test_fallback_switch_records_exhaustion_for_next_session(self):
        drv = AntigravityDriver(cwd="/tmp", model="gemini-default", fallback_models=["sonnet"])
        drv.terminate_session = mock.AsyncMock()
        new_model = await drv._switch_to_fallback_model(QUOTA_MSG)
        self.assertEqual(new_model, "sonnet")
        self.assertTrue(is_model_exhausted("gemini-default"))
        # A replacement session now starts straight on sonnet: no second failed spawn.
        nxt = AntigravityDriver(cwd="/tmp", model="gemini-default", fallback_models=["sonnet"])
        self.assertEqual(nxt.model, "sonnet")

    async def test_switch_skips_fallbacks_already_exhausted(self):
        mark_model_exhausted("dead", QUOTA_MSG)
        drv = AntigravityDriver(cwd="/tmp", model="primary", fallback_models=["dead", "alive"])
        drv.terminate_session = mock.AsyncMock()
        self.assertEqual(await drv._switch_to_fallback_model(QUOTA_MSG), "alive")

    async def test_exhaustion_recorded_even_without_fallback(self):
        from golem.protocol import FinishEvent

        drv = AntigravityDriver(cwd="/tmp", model="only-model", fallback_models=[])

        async def fake_turn(prompt):
            yield FinishEvent(response="", status="ERROR", error=QUOTA_MSG)

        drv._stream_turn_impl = fake_turn
        events = [e async for e in drv.send_prompt("go")]
        self.assertTrue(any(isinstance(e, FinishEvent) and e.status == "ERROR" for e in events))
        self.assertTrue(is_model_exhausted("only-model"))


class LinearNotesBlockTests(unittest.TestCase):
    def test_empty_notes_yield_nothing(self):
        self.assertEqual(build_linear_notes_block(None), "")
        self.assertEqual(build_linear_notes_block({}), "")

    def test_block_contains_findings_and_subtasks(self):
        block = build_linear_notes_block({
            "identifier": "CHO-166",
            "title": "golem.horned.se",
            "state": "In Progress",
            "subtasks": ["[CHO-167] Find tunnel -> Done"],
            "comments": [{"body": "Hand result: tunnel runs on oci-machine, WebUI port 8765"}],
        })
        self.assertTrue(block.startswith("[Linear working notes: CHO-166"))
        self.assertIn("CHO-167", block)
        self.assertIn("port 8765", block)
        self.assertTrue(block.endswith("]"))

    def test_block_is_bounded(self):
        block = build_linear_notes_block(
            {"identifier": "CHO-1", "title": "t", "state": "s",
             "comments": [{"body": "x" * 900} for _ in range(20)]},
            max_chars=1000,
        )
        self.assertLess(len(block), 1100)


class ReadOnlyHopCapTests(unittest.TestCase):
    def test_strip_removes_only_read_only_tools(self):
        original = get_mouth_tools_declaration()
        stripped = strip_read_only_tools(original)
        names = {d["name"] for g in stripped for d in g["functionDeclarations"]}
        self.assertFalse(names & READ_ONLY_HOP_TOOLS)
        self.assertIn("dispatch_task", names)
        self.assertIn("plan_task_with_subtasks", names)
        # original declaration is not mutated
        original_names = {d["name"] for g in original for d in g["functionDeclarations"]}
        self.assertIn("read_project_file", original_names)


class IdlePromptAnnotationTests(unittest.TestCase):
    def _status(self, **kw):
        base = {"state": "IDLE", "active_command": "", "command": "", "pid": None,
                "blocker": None, "blockers": [], "buffer_tail": ""}
        base.update(kw)
        return base

    def test_bare_prompt_is_annotated_and_blanked(self):
        res = annotate_idle_shell_prompt(self._status(buffer_tail="hermes@hermes AGY %\n"))
        self.assertEqual(res["buffer_tail"], "")
        self.assertIn("did not crash", res["note"])

    def test_real_output_is_left_alone(self):
        tail = "Traceback (most recent call last):\nValueError: boom\nhermes@hermes AGY %"
        res = annotate_idle_shell_prompt(self._status(buffer_tail=tail))
        self.assertEqual(res["buffer_tail"], tail)
        self.assertNotIn("note", res)

    def test_non_idle_states_are_left_alone(self):
        for state in ("RUNNING", "BLOCKER", "COMPLETED", "ERROR"):
            res = annotate_idle_shell_prompt(self._status(state=state, buffer_tail="user@host %"))
            self.assertNotIn("note", res)

    def test_active_command_is_left_alone(self):
        res = annotate_idle_shell_prompt(self._status(active_command="ls", buffer_tail="user@host %"))
        self.assertNotIn("note", res)


class HandBriefAndEmptyCandidateTests(unittest.TestCase):
    def test_goal_leads_and_command_is_a_hint(self):
        brief = compose_hand_brief("Expose the WebUI at golem.horned.se", "cloudflared tunnel list")
        self.assertTrue(brief.startswith("Expose the WebUI"))
        self.assertIn("Suggested starting command", brief)
        self.assertIn("cloudflared tunnel list", brief)

    def test_goal_only_and_command_only(self):
        self.assertEqual(compose_hand_brief("do the thing", None), "do the thing")
        self.assertEqual(compose_hand_brief(None, "ls -la"), "ls -la")
        self.assertEqual(compose_hand_brief("", ""), "")

    def test_dispatch_schema_is_goal_first(self):
        from golem.mouth import DISPATCH_TASK_TOOL_DECLARATION as decl
        self.assertEqual(decl["parameters"]["required"], ["task_description"])
        self.assertIn("command", decl["parameters"]["properties"])
        self.assertNotIn("exact shell command", decl["description"].lower())

    def test_runner_checks_are_chainable(self):
        from golem.ingress import CHAINABLE_TOOLS
        self.assertIn("check_eyes", CHAINABLE_TOOLS)
        self.assertIn("check_nose", CHAINABLE_TOOLS)
        self.assertNotIn("dispatch_task", CHAINABLE_TOOLS)

    def test_candidate_is_empty(self):
        def resp(parts):
            return {"candidates": [{"finishReason": "STOP", "content": {"parts": parts}}]}

        self.assertTrue(candidate_is_empty(resp([])))
        self.assertTrue(candidate_is_empty(resp([{"text": "pondering", "thought": True}])))
        self.assertTrue(candidate_is_empty({"candidates": []}))
        self.assertFalse(candidate_is_empty(resp([{"text": "hello"}])))
        self.assertFalse(candidate_is_empty(resp([{"functionCall": {"name": "dispatch_task", "args": {}}}])))
        self.assertFalse(candidate_is_empty({"_error": True}))
        self.assertFalse(candidate_is_empty(None))

    def test_describe_candidate_shape_reports_finish_reason(self):
        shape = describe_candidate_shape({"candidates": [{
            "finishReason": "MALFORMED_FUNCTION_CALL",
            "content": {"parts": [{"functionCall": {"name": "dispatch_task"}}, {"text": "x", "thought": True}]},
        }]})
        self.assertIn("MALFORMED_FUNCTION_CALL", shape)
        self.assertIn("call:dispatch_task", shape)
        self.assertIn("thought", shape)


class MouthTurnLinearMemoryTests(unittest.IsolatedAsyncioTestCase):
    def _gateway(self):
        return object.__new__(IngressGateway)

    async def test_turn_gets_notes_and_scopes_thread_ref(self):
        gw = self._gateway()
        seen = {}

        async def fake_run(session, prompt, client, channel, thread_ts, ts, is_imessage):
            seen["prompt"] = prompt
            seen["ref"] = _THREAD_REF.get()

        gw._run_mouth_turn = fake_run
        gw._load_linear_notes_block = mock.AsyncMock(return_value="[Linear working notes: CHO-1]")
        await gw._handle_mouth_turn(None, "continue", None, "D1", "99.9", "100.0", False)
        self.assertTrue(seen["prompt"].startswith("[Linear working notes: CHO-1]"))
        self.assertTrue(seen["prompt"].endswith("continue"))
        self.assertEqual(seen["ref"], "D1:99.9")
        self.assertEqual(_THREAD_REF.get(), "")  # reset: nothing leaks into the next turn

    async def test_turn_without_thread_skips_linear(self):
        gw = self._gateway()
        gw._run_mouth_turn = mock.AsyncMock()
        gw._load_linear_notes_block = mock.AsyncMock(return_value="x")
        await gw._handle_mouth_turn(None, "hi", None, "D1", "", "1.0", False)
        gw._load_linear_notes_block.assert_not_awaited()
        self.assertEqual(gw._run_mouth_turn.call_args.args[1], "hi")

    async def test_hand_result_is_commented_on_thread_issue(self):
        gw = self._gateway()
        bridge = mock.MagicMock(api_key="k")
        bridge.ensure_thread_issue = mock.AsyncMock(return_value={"id": "i1"})
        bridge.add_comment = mock.AsyncMock(return_value=True)
        gw.linear_bridge = bridge
        token = _THREAD_REF.set("D1:99.9")
        try:
            with mock.patch("golem.ingress._linear_disabled_for_tests", return_value=False):
                gw._record_hand_result_to_linear(
                    "check tunnel", "oci-machine", {"status": "SUCCESS", "output_tail": "dev.horned.se -> 5173"}
                )
                await asyncio.sleep(0.05)
        finally:
            _THREAD_REF.reset(token)
        bridge.add_comment.assert_awaited_once()
        body = bridge.add_comment.call_args.args[1]
        self.assertIn("oci-machine", body)
        self.assertIn("dev.horned.se -> 5173", body)

    async def test_hand_result_ignored_without_thread(self):
        gw = self._gateway()
        bridge = mock.MagicMock(api_key="k")
        bridge.ensure_thread_issue = mock.AsyncMock()
        gw.linear_bridge = bridge
        with mock.patch("golem.ingress._linear_disabled_for_tests", return_value=False):
            gw._record_hand_result_to_linear("t", "n", {"status": "SUCCESS"})
            await asyncio.sleep(0.02)
        bridge.ensure_thread_issue.assert_not_awaited()


class LinearBridgeThreadTests(unittest.IsolatedAsyncioTestCase):
    def _bridge(self):
        b = LinearBridge(api_key="test-key")
        b._project_id_cache, b._team_id_cache = "proj", "team"
        b._state_cache = {"in progress": "s-ip", "started": "s-ip"}
        return b

    async def test_create_task_issue_sets_parent_and_state(self):
        b = self._bridge()
        b.execute_query = mock.AsyncMock(return_value={"issueCreate": {"issue": {"id": "i1", "identifier": "CHO-9"}}})
        issue = await b.create_task_issue("title", "desc", parent_id="p1")
        self.assertEqual(issue["identifier"], "CHO-9")
        sent = b.execute_query.call_args.args[1]["input"]
        self.assertEqual(sent["parentId"], "p1")
        self.assertEqual(sent["stateId"], "s-ip")
        self.assertEqual(sent["projectId"], "proj")

    async def test_find_thread_issue_none_when_untracked(self):
        b = self._bridge()
        b.execute_query = mock.AsyncMock(return_value={"project": {"issues": {"nodes": []}}})
        self.assertIsNone(await b.find_thread_issue("C1:123.4"))
        self.assertEqual(b.execute_query.call_args.args[1]["ref"], "Thread: C1:123.4")

    async def test_ensure_thread_issue_creates_once(self):
        b = self._bridge()
        b.find_thread_issue = mock.AsyncMock(side_effect=[None, {"id": "i1", "identifier": "CHO-9"}])
        b.create_task_issue = mock.AsyncMock(return_value={"id": "i1", "identifier": "CHO-9"})
        first = await b.ensure_thread_issue("C1:1", "t", "d")
        second = await b.ensure_thread_issue("C1:1", "t", "d")
        self.assertEqual(first["identifier"], second["identifier"])
        b.create_task_issue.assert_awaited_once()
        self.assertTrue(b.create_task_issue.call_args.args[1].startswith("Thread: C1:1"))

    async def test_get_thread_notes_none_when_untracked(self):
        b = self._bridge()
        b.find_thread_issue = mock.AsyncMock(return_value=None)
        self.assertIsNone(await b.get_thread_notes("C1:1"))

    async def test_get_thread_notes_shapes_result(self):
        b = self._bridge()
        b.find_thread_issue = mock.AsyncMock(return_value={"id": "i1", "identifier": "CHO-9", "title": "t",
                                                           "state": {"name": "In Progress"}})
        b.execute_query = mock.AsyncMock(return_value={"issue": {
            "identifier": "CHO-9", "title": "t", "state": {"name": "In Progress"},
            "children": {"nodes": [{"identifier": "CHO-10", "title": "sub", "state": {"name": "Planned"}}]},
            "comments": {"nodes": [{"body": "port 8765", "createdAt": "2026-10-12T00:00:00Z"}]},
        }})
        notes = await b.get_thread_notes("C1:1")
        self.assertEqual(notes["identifier"], "CHO-9")
        self.assertEqual(notes["subtasks"], ["[CHO-10] sub -> Planned"])
        self.assertEqual(notes["comments"][0]["body"], "port 8765")


if __name__ == "__main__":
    unittest.main()
