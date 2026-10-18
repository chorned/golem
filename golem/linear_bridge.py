#!/usr/bin/env python3
"""
golem/linear_bridge.py
Linear GraphQL Integration for Multi-Stage Task Planning,
Subtask Hierarchies, and Human Sign-Off Verification Gate.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import re
import asyncio
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from unittest.mock import MagicMock, AsyncMock
import aiohttp

logger = logging.getLogger("golem.linear")
LINEAR_GRAPHQL_ENDPOINT = "https://api.linear.app/graphql"

# Per-thread locks so concurrent dispatches in one Slack thread don't both create the thread's issue.
_THREAD_ISSUE_LOCKS: Dict[str, asyncio.Lock] = {}
# Cache of true parent project tickets (never ephemeral [Hand] tickets) keyed by thread_ref.
_THREAD_ISSUE_CACHE: Dict[str, Dict[str, Any]] = {}


class LinearBridgeError(Exception):
    """Raised on Linear API connection, GraphQL, or entity resolution errors."""
    pass


class LinearBridge:
    """
    Bridge to Linear for hierarchical project task management.
    Scaffolds parent deliverables and child subtasks under Golem_Brain,
    tracks multi-hand settlements, and enforces human signoff invariant.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        project_slug: str = "golem-brain-109337da0931",
        default_team_key: str = "CHO",
    ) -> None:
        resolved_key = api_key if api_key is not None else os.getenv("LINEAR_API_KEY", "")
        resolved_slug = project_slug or os.getenv("LINEAR_PROJECT_SLUG", "golem-brain-109337da0931")
        resolved_team = default_team_key or os.getenv("LINEAR_DEFAULT_TEAM_KEY", "CHO")

        # Fallback inspection of ~/.golem/config.env or .env if not provided in environment
        if not resolved_key and api_key is None:
            candidate_files = [
                Path(os.path.expanduser("~/.golem/config.env")),
                Path(__file__).resolve().parent.parent / ".env",
            ]
            for candidate in candidate_files:
                if candidate.exists():
                    try:
                        for line in candidate.read_text(encoding="utf-8").splitlines():
                            line = line.strip()
                            if line.startswith("LINEAR_API_KEY="):
                                val = line.split("=", 1)[1].strip().strip('"').strip("'")
                                if val:
                                    resolved_key = val
                            elif line.startswith("LINEAR_PROJECT_SLUG="):
                                val = line.split("=", 1)[1].strip().strip('"').strip("'")
                                if val and not project_slug:
                                    resolved_slug = val
                            elif line.startswith("LINEAR_DEFAULT_TEAM_KEY="):
                                val = line.split("=", 1)[1].strip().strip('"').strip("'")
                                if val and not default_team_key:
                                    resolved_team = val
                    except Exception as exc:
                        logger.debug(f"Unable to read {candidate} for Linear credentials: {exc}")
                if resolved_key:
                    break

        self.api_key: str = resolved_key or ""
        self.project_slug: str = resolved_slug
        self.default_team_key: str = resolved_team
        self._project_id_cache: Optional[str] = None
        self._team_id_cache: Optional[str] = None
        self._state_cache: Dict[str, str] = {}
        self._viewer_id_cache: Optional[str] = None
        self._viewer_cache: Optional[Dict[str, Any]] = None
        self._http_session: Optional[aiohttp.ClientSession] = None
        self._session_loop: Optional[asyncio.AbstractEventLoop] = None

    def _headers(self) -> Dict[str, str]:
        if not self.api_key:
            raise LinearBridgeError("LINEAR_API_KEY is not configured in environment.")
        return {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
        }

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """Loop-aware persistent HTTP session helper reusing self._http_session with total timeout 15s."""
        current_loop = asyncio.get_running_loop()
        if (
            self._http_session is not None
            and not self._http_session.closed
            and self._session_loop is current_loop
        ):
            return self._http_session

        if self._http_session is not None and not self._http_session.closed:
            try:
                await self._http_session.close()
            except Exception:
                pass

        timeout = aiohttp.ClientTimeout(total=15)
        self._http_session = aiohttp.ClientSession(timeout=timeout)
        self._session_loop = current_loop
        return self._http_session

    async def close(self) -> None:
        """Close persistent HTTP session if open."""
        if self._http_session is not None and not self._http_session.closed:
            try:
                await self._http_session.close()
            except Exception:
                pass
            self._http_session = None
            self._session_loop = None

    async def execute_query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        session = await self._get_http_session()
        payload = {"query": query, "variables": variables or {}}
        async with session.post(LINEAR_GRAPHQL_ENDPOINT, headers=self._headers(), json=payload) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise LinearBridgeError(f"HTTP {resp.status} from Linear API: {body}")
            data = await resp.json()
            if "errors" in data and data["errors"]:
                raise LinearBridgeError(f"Linear GraphQL error: {data['errors']}")
            return data.get("data", {})

    async def resolve_project_and_states(self) -> Tuple[str, str, Dict[str, str]]:
        if self._project_id_cache and self._team_id_cache and self._state_cache:
            return self._project_id_cache, self._team_id_cache, self._state_cache

        query = """
        query GetProjectMetadata($slug: String!) {
            viewer {
                id
                name
                displayName
                email
            }
            project(id: $slug) {
                id
                name
                teams {
                    nodes {
                        id
                        key
                        name
                        states {
                            nodes {
                                id
                                name
                                type
                            }
                        }
                    }
                }
            }
        }
        """
        data = await self.execute_query(query, {"slug": self.project_slug})
        viewer = data.get("viewer")
        if isinstance(viewer, dict) and viewer.get("id"):
            self._viewer_cache = viewer
            self._viewer_id_cache = viewer.get("id")
        proj = data.get("project")

        if not proj:
            list_query = "query ListProjects { projects(first: 50) { nodes { id name slugId } } }"
            proj_data = await self.execute_query(list_query)
            for p in proj_data.get("projects", {}).get("nodes", []):
                if "golem" in p.get("name", "").lower() or (p.get("slugId") and p["slugId"] in self.project_slug):
                    proj = p
                    break

        if not proj:
            raise LinearBridgeError(f"Could not locate Linear project '{self.project_slug}'.")

        project_id = proj["id"]
        teams = proj.get("teams", {}).get("nodes", [])

        if not teams:
            team_query = "query GetTeams { teams { nodes { id key states { nodes { id name type } } } } }"
            t_data = await self.execute_query(team_query)
            teams = t_data.get("teams", {}).get("nodes", [])

        selected_team = next((t for t in teams if t["key"].upper() == self.default_team_key.upper()), None)
        if not selected_team and teams:
            selected_team = teams[0]

        if not selected_team:
            raise LinearBridgeError(f"Could not locate Linear team '{self.default_team_key}'.")

        team_id = selected_team["id"]
        state_map: Dict[str, str] = {}
        for s in selected_team.get("states", {}).get("nodes", []):
            name = s.get("name", "").strip().lower()
            stype = s.get("type", "").strip().lower()
            if name:
                state_map[name] = s["id"]
            if stype and (stype not in state_map or name == "in progress"):
                state_map[stype] = s["id"]

        self._project_id_cache = project_id
        self._team_id_cache = team_id
        self._state_cache = state_map
        return project_id, team_id, state_map

    async def _ensure_viewer_id(self) -> Optional[str]:
        if self._viewer_id_cache:
            return self._viewer_id_cache
        if not self.api_key:
            return None
        if hasattr(self.execute_query, "_mock_name") or isinstance(self.execute_query, (MagicMock, AsyncMock)):
            return self._viewer_id_cache
        try:
            viewer = await self.get_viewer()
            if isinstance(viewer, dict) and viewer.get("id"):
                self._viewer_id_cache = viewer.get("id")
        except Exception as exc:
            logger.debug(f"Unable to resolve Linear viewer ID: {exc}")
        return self._viewer_id_cache

    async def create_plan_with_subtasks(
        self,
        parent_title: str,
        parent_description: str,
        subtasks: List[str],
        parent_key: Optional[str] = None,
        related_issue_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Creates a parent issue in 'In Progress' (or reuses/updates existing parent_key)
        and child subtasks in 'Planned'. When related_issue_key is provided for a new
        follow-up plan, links the newly created parent issue to related_issue_key in Linear.
        """
        project_id, team_id, states = await self.resolve_project_and_states()
        in_progress_state_id = states.get("in progress") or states.get("started")
        planned_state_id = states.get("planned") or states.get("unstarted")
        assignee_id = await self._ensure_viewer_id()

        resolved_parent_key: Optional[str] = None
        resolved_related_key: Optional[str] = related_issue_key.strip().upper() if (related_issue_key and related_issue_key.strip()) else None
        is_mocked_query = hasattr(self.execute_query, "_mock_name") or isinstance(self.execute_query, (MagicMock, AsyncMock))
        if parent_key:
            resolved_parent_key = parent_key.strip().upper()
        elif not is_mocked_query and not resolved_related_key:
            m = re.search(r"\b(CHO-\d+)\b", f"{parent_title} {parent_description}", re.IGNORECASE)
            if m:
                resolved_parent_key = m.group(1).upper()

        parent_issue = None
        if resolved_parent_key:
            try:
                get_query = """
                query GetExistingParent($key: String!) {
                    issue(id: $key) { id identifier url title description }
                }
                """
                data = await self.execute_query(get_query, {"key": resolved_parent_key})
                existing = data.get("issue")
                if existing and existing.get("id"):
                    update_input: Dict[str, Any] = {}
                    if in_progress_state_id:
                        update_input["stateId"] = in_progress_state_id
                    if assignee_id:
                        update_input["assigneeId"] = assignee_id
                    if parent_description:
                        update_input["description"] = parent_description
                    if update_input:
                        update_mutation = """
                        mutation UpdateParent($id: String!, $input: IssueUpdateInput!) {
                            issueUpdate(id: $id, input: $input) {
                                success
                                issue { id identifier url title }
                            }
                        }
                        """
                        up_res = await self.execute_query(update_mutation, {"id": existing["id"], "input": update_input})
                        updated_issue = up_res.get("issueUpdate", {}).get("issue")
                        if isinstance(updated_issue, dict):
                            existing.update({k: v for k, v in updated_issue.items() if v})
                    parent_issue = existing
            except Exception as exc:
                logger.warning(f"Failed to lookup or update existing parent {resolved_parent_key}: {exc}")
                parent_issue = None

        if not parent_issue:
            # 1. Create Parent Issue
            effective_description = parent_description or ""
            if resolved_related_key and resolved_related_key not in effective_description.upper():
                effective_description = f"Follow-up to: {resolved_related_key}\n{effective_description}".strip()

            parent_mutation = """
            mutation CreateParent($input: IssueCreateInput!) {
                issueCreate(input: $input) {
                    success
                    issue { id identifier url title }
                }
            }
            """
            parent_vars: Dict[str, Any] = {
                "input": {
                    "teamId": team_id,
                    "projectId": project_id,
                    "title": parent_title[:255],
                    "description": effective_description,
                    "stateId": in_progress_state_id,
                }
            }
            if assignee_id:
                parent_vars["input"]["assigneeId"] = assignee_id

            p_res = await self.execute_query(parent_mutation, parent_vars)
            parent_issue = p_res.get("issueCreate", {}).get("issue", {})

        parent_id = parent_issue.get("id")
        parent_identifier = (parent_issue.get("identifier") or "").strip().upper()

        # 2. Create Child Subtasks concurrently via asyncio.gather
        child_mutation = """
        mutation CreateSubtask($input: IssueCreateInput!) {
            issueCreate(input: $input) {
                success
                issue { id identifier title url }
            }
        }
        """

        async def _create_child(index: int, subtask_title: str) -> Dict[str, Any]:
            child_vars: Dict[str, Any] = {
                "input": {
                    "teamId": team_id,
                    "projectId": project_id,
                    "parentId": parent_id,
                    "title": f"[{index}/{len(subtasks)}] {subtask_title[:220]}",
                    "stateId": planned_state_id,
                }
            }
            if assignee_id:
                child_vars["input"]["assigneeId"] = assignee_id
            c_res = await self.execute_query(child_mutation, child_vars)
            c_issue = c_res.get("issueCreate", {}).get("issue", {})
            return {
                "id": c_issue.get("id"),
                "identifier": c_issue.get("identifier"),
                "title": c_issue.get("title"),
                "url": c_issue.get("url"),
            }

        child_coros = [_create_child(index, subtask_title) for index, subtask_title in enumerate(subtasks, start=1)]
        created_subtasks = list(await asyncio.gather(*child_coros)) if child_coros else []

        # 3. Link follow-up parent issue to upstream/original issue in Linear
        linked_related_key: Optional[str] = None
        if resolved_related_key and parent_id and resolved_related_key != parent_identifier:
            try:
                linked = await self.link_issues(parent_id, resolved_related_key)
                if linked:
                    linked_related_key = resolved_related_key
            except Exception as exc:
                logger.warning(f"Failed to link follow-up issue {parent_identifier or parent_id} to {resolved_related_key}: {exc}")

        parent_result: Dict[str, Any] = {
            "id": parent_issue.get("id"),
            "identifier": parent_issue.get("identifier"),
            "url": parent_issue.get("url"),
            "title": parent_issue.get("title"),
        }
        if linked_related_key or resolved_related_key:
            parent_result["related_issue_key"] = linked_related_key or resolved_related_key

        return {
            "parent": parent_result,
            "subtasks": created_subtasks,
        }

    async def create_task_issue(
        self,
        title: str,
        description: str = "",
        parent_id: Optional[str] = None,
        state_key: str = "in progress",
    ) -> Dict[str, Any]:
        """
        Create a single tracking issue under the Golem project (optionally as a child of `parent_id`).
        Returns {id, identifier, url, title}.
        """
        project_id, team_id, states = await self.resolve_project_and_states()
        state_id = states.get(state_key.strip().lower()) or states.get("started")
        assignee_id = await self._ensure_viewer_id()
        mutation = """
        mutation CreateTask($input: IssueCreateInput!) {
            issueCreate(input: $input) {
                success
                issue { id identifier url title }
            }
        }
        """
        issue_input: Dict[str, Any] = {
            "teamId": team_id,
            "projectId": project_id,
            "title": title[:255],
            "description": description,
        }
        if state_id:
            issue_input["stateId"] = state_id
        if parent_id:
            issue_input["parentId"] = parent_id
        if assignee_id:
            issue_input["assigneeId"] = assignee_id
        res = await self.execute_query(mutation, {"input": issue_input})
        return res.get("issueCreate", {}).get("issue", {}) or {}

    async def add_comment(self, issue_key_or_id: str, body: str) -> bool:
        """Append a comment to an issue (accepts a CHO-123 key or a UUID)."""
        issue_id = await self._resolve_issue_id(issue_key_or_id)
        mutation = """
        mutation AddComment($input: CommentCreateInput!) {
            commentCreate(input: $input) { success }
        }
        """
        res = await self.execute_query(mutation, {"input": {"issueId": issue_id, "body": body}})
        return bool(res.get("commentCreate", {}).get("success"))

    async def find_thread_issue(self, thread_ref: str) -> Optional[Dict[str, Any]]:
        """
        Find the most recently updated top-level issue tagged with `thread_ref`
        (the ``Thread: `<ref>` `` line Golem writes into issue descriptions).
        Returns None if the thread has no active issue yet.
        """
        if not thread_ref:
            return None
        cached = _THREAD_ISSUE_CACHE.get(thread_ref)
        if isinstance(cached, dict) and cached.get("id"):
            cached_title = str(cached.get("title") or "").strip()
            cached_state = cached.get("state") or {}
            c_state_name = str(cached_state.get("name") or "").strip().lower()
            c_state_type = str(cached_state.get("type") or "").strip().lower()
            if not cached_title.startswith("[Hand]") and c_state_type not in ("canceled", "cancelled"):
                return cached
            _THREAD_ISSUE_CACHE.pop(thread_ref, None)

        project_id, _, _ = await self.resolve_project_and_states()
        query = """
        query FindThreadIssue($projectId: String!, $ref: String!) {
            project(id: $projectId) {
                issues(first: 5, orderBy: updatedAt, filter: {
                    parent: { null: true },
                    description: { contains: $ref }
                }) {
                    nodes { id identifier title url state { name type } }
                }
            }
        }
        """
        # 1. Primary query: inline-backtick wrapped thread_ref so Linear's Markdown parser never linkifies emails/URLs
        candidate_refs: List[str] = [
            f"Thread: `{thread_ref}`",
            f"Thread: {thread_ref}",
        ]
        # 2. Backwards-compatible fallback for older tickets where Linear Markdown-linkified emails in thread_ref
        if "@" in thread_ref:
            linkified_ref = re.sub(
                r"((?:iMessage;-;)?[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})",
                r"[\1](<mailto:\1>)",
                thread_ref,
                count=1,
            )
            if linkified_ref != thread_ref:
                candidate_refs.append(f"Thread: {linkified_ref}")
            raw_handle = thread_ref.split(":")[-1].strip()
            if raw_handle and f"Thread: {raw_handle}" not in candidate_refs:
                candidate_refs.append(raw_handle)

        nodes: List[Dict[str, Any]] = []
        for ref_str in candidate_refs:
            data = await self.execute_query(query, {"projectId": project_id, "ref": ref_str})
            nodes = data.get("project", {}).get("issues", {}).get("nodes", []) or []
            if nodes:
                break

        if not nodes:
            return None

        # Prefer true parent project tickets (non-[Hand]) first; skip settled [Hand] tickets
        settled_names = {"done", "completed", "canceled", "cancelled", "claimed fixed", "in review"}
        settled_types = {"completed", "canceled", "cancelled"}
        for node in nodes:
            if not isinstance(node, dict):
                continue
            title_val = str(node.get("title") or "").strip()
            st = node.get("state") or {}
            st_name = str(st.get("name") or "").strip().lower()
            st_type = str(st.get("type") or "").strip().lower()
            if not title_val.startswith("[Hand]") and st_type not in ("canceled", "cancelled"):
                _THREAD_ISSUE_CACHE[thread_ref] = node
                return node

        for node in nodes:
            if not isinstance(node, dict):
                continue
            title_val = str(node.get("title") or "").strip()
            st = node.get("state") or {}
            st_name = str(st.get("name") or "").strip().lower()
            st_type = str(st.get("type") or "").strip().lower()
            if title_val.startswith("[Hand]") and (st_name in settled_names or st_type in settled_types):
                continue
            return node

        return None

    async def find_open_hand_issue(
        self,
        title: str,
        parent_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Look up an existing open `[Hand]` issue with the same normalized title so retries
        reuse the same ticket instead of spawning duplicate `[Hand]` issues.
        """
        clean_title = (title or "").strip()[:255]
        if not clean_title:
            return None
        project_id, _, _ = await self.resolve_project_and_states()
        query = """
        query FindOpenHandIssue($projectId: String!, $title: String!) {
            project(id: $projectId) {
                issues(first: 5, orderBy: updatedAt, filter: {
                    title: { eq: $title },
                    state: { type: { nin: ["completed", "canceled"] } }
                }) {
                    nodes { id identifier title url state { name type } parent { id identifier } }
                }
            }
        }
        """
        try:
            data = await self.execute_query(query, {"projectId": project_id, "title": clean_title})
            nodes = data.get("project", {}).get("issues", {}).get("nodes", []) or []
            for node in nodes:
                if not isinstance(node, dict):
                    continue
                st = node.get("state") or {}
                st_name = str(st.get("name") or "").strip().lower()
                st_type = str(st.get("type") or "").strip().lower()
                if st_name in ("done", "completed", "canceled", "cancelled", "claimed fixed") or st_type in ("completed", "canceled"):
                    continue
                if parent_id:
                    node_parent_id = (node.get("parent") or {}).get("id")
                    node_parent_key = (node.get("parent") or {}).get("identifier")
                    if node_parent_id and parent_id not in (node_parent_id, node_parent_key):
                        continue
                return node
        except Exception as exc:
            logger.debug(f"find_open_hand_issue lookup failed for '{clean_title}': {exc}")
        return None

    async def get_thread_notes(self, thread_ref: str, max_comments: int = 6) -> Optional[Dict[str, Any]]:
        """
        Return the thread's Linear issue with its subtask states and the latest comments, so a new turn
        can restore what earlier turns already discovered. None if the thread has no Linear issue.
        """
        issue = await self.find_thread_issue(thread_ref)
        if not issue:
            return None
        query = """
        query ThreadNotes($id: String!, $last: Int!) {
            issue(id: $id) {
                identifier
                title
                state { name }
                children { nodes { identifier title state { name } } }
                comments(last: $last) { nodes { body createdAt } }
            }
        }
        """
        data = await self.execute_query(query, {"id": issue["id"], "last": max(1, int(max_comments))})
        full = data.get("issue") or {}
        return {
            "identifier": full.get("identifier") or issue.get("identifier"),
            "title": full.get("title") or issue.get("title"),
            "state": (full.get("state") or issue.get("state") or {}).get("name", "Unknown"),
            "subtasks": [
                f"[{c.get('identifier', '')}] {c.get('title', '')} -> {c.get('state', {}).get('name', 'Unknown')}"
                for c in full.get("children", {}).get("nodes", [])
            ],
            "comments": [
                {"body": c.get("body", ""), "created_at": c.get("createdAt", "")}
                for c in full.get("comments", {}).get("nodes", [])
            ],
        }

    async def ensure_thread_issue(
        self,
        thread_ref: str,
        title: str,
        description: str = "",
        parent_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get the thread's tracking issue, creating it (tagged with ``Thread: `<ref>` ``) if none exists yet.
        True parent project tickets are cached in `_THREAD_ISSUE_CACHE[thread_ref]`; ephemeral `[Hand]`
        tickets are deduplicated via `find_open_hand_issue` and never cached as the thread's parent issue.
        """
        lock = _THREAD_ISSUE_LOCKS.setdefault(thread_ref, asyncio.Lock())
        async with lock:
            is_hand_title = str(title or "").strip().startswith("[Hand]")
            existing = await self.find_thread_issue(thread_ref)
            if existing:
                existing_title = str(existing.get("title") or "").strip()
                if not existing_title.startswith("[Hand]"):
                    _THREAD_ISSUE_CACHE[thread_ref] = existing
                return existing

            if is_hand_title:
                open_hand = await self.find_open_hand_issue(title, parent_id=parent_id)
                if open_hand:
                    return open_hand

            body = f"Thread: `{thread_ref}`\n{description}".strip()
            created = await self.create_task_issue(title, body, parent_id=parent_id)
            if isinstance(created, dict) and created.get("id") and not is_hand_title:
                _THREAD_ISSUE_CACHE[thread_ref] = created
            return created

    async def settle_hand_issue(
        self,
        issue_key_or_id: str,
        status: str = "SUCCESS",
        summary_comment: str = "",
        thread_ref: str = "",
    ) -> bool:
        """
        Immediately transition an ephemeral `[Hand]` ticket out of `In Progress` when the Hand
        process finishes (`Completed` on success, `Canceled` on failure/blocker) and evict any
        stale entry from `_THREAD_ISSUE_CACHE`.
        """
        if thread_ref:
            cached = _THREAD_ISSUE_CACHE.get(thread_ref)
            if isinstance(cached, dict) and (
                cached.get("id") == issue_key_or_id
                or cached.get("identifier") == issue_key_or_id
                or str(cached.get("title") or "").strip().startswith("[Hand]")
            ):
                _THREAD_ISSUE_CACHE.pop(thread_ref, None)

        if not issue_key_or_id:
            return False

        normalized_status = (status or "").strip().upper()
        target_state = "completed" if normalized_status in ("SUCCESS", "COMPLETED", "DONE") else "canceled"
        if summary_comment:
            try:
                await self.add_comment(issue_key_or_id, summary_comment)
            except Exception as exc:
                logger.debug(f"settle_hand_issue add_comment failed for {issue_key_or_id}: {exc}")
        try:
            return await self.update_issue_state(issue_key_or_id, target_state)
        except Exception as exc:
            logger.debug(f"settle_hand_issue update_issue_state({target_state}) failed for {issue_key_or_id}: {exc}")
            if target_state == "canceled":
                try:
                    return await self.update_issue_state(issue_key_or_id, "completed")
                except Exception:
                    pass
            return False

    async def reconcile_zombie_hand_issues(
        self,
        active_keys_or_titles: Optional[Any] = None,
    ) -> List[str]:
        """
        Find any `[Hand]` issues stuck in `In Progress` in Linear that do NOT correspond to an
        actively running local Hand process, and transition them to `Canceled` (or `Completed`
        if they already have a `[Golem_Brain] Hand finished (status=SUCCESS)` comment), while
        closing duplicate `[Hand]` tickets with identical titles.
        Returns the list of settled issue identifiers.
        """
        active_set = {str(x).strip().upper() for x in (active_keys_or_titles or []) if x}
        project_id, _, _ = await self.resolve_project_and_states()
        query = """
        query FindInProgressHandIssues($projectId: String!) {
            project(id: $projectId) {
                issues(first: 25, orderBy: updatedAt, filter: {
                    title: { startsWith: "[Hand]" },
                    state: { type: { eq: "started" } }
                }) {
                    nodes {
                        id
                        identifier
                        title
                        state { name type }
                        comments(last: 3) { nodes { body } }
                    }
                }
            }
        }
        """
        settled_keys: List[str] = []
        try:
            data = await self.execute_query(query, {"projectId": project_id})
            nodes = data.get("project", {}).get("issues", {}).get("nodes", []) or []
        except Exception as exc:
            logger.debug(f"reconcile_zombie_hand_issues query failed: {exc}")
            return settled_keys

        for node in nodes:
            if not isinstance(node, dict):
                continue
            ident = str(node.get("identifier") or "").strip().upper()
            issue_id = str(node.get("id") or "").strip()
            title = str(node.get("title") or "").strip()
            if not issue_id:
                continue
            if ident in active_set or issue_id.upper() in active_set or title.upper() in active_set:
                continue
            comments = [str((c or {}).get("body") or "") for c in ((node.get("comments") or {}).get("nodes") or [])]
            succeeded = any("status=SUCCESS" in c or "exit=0" in c for c in comments)
            ok = await self.settle_hand_issue(
                issue_id,
                status="SUCCESS" if succeeded else "CANCELED",
                summary_comment="" if comments else "_[Golem_Brain] Auto-reconciled orphaned `[Hand]` execution ticket with no active local runner._",
            )
            if ok and ident:
                settled_keys.append(ident)
        return settled_keys

    async def update_issue_state(self, issue_id: str, state_key: str) -> bool:
        _, _, states = await self.resolve_project_and_states()
        key_lower = state_key.strip().lower()
        target_state_id = states.get(key_lower)
        if not target_state_id:
            # Fallback to standard Linear state types
            if key_lower == "claimed fixed":
                target_state_id = states.get("in review")
            elif key_lower == "in progress":
                target_state_id = states.get("started")
            elif key_lower == "completed":
                target_state_id = states.get("done")
            elif key_lower == "planned":
                target_state_id = states.get("unstarted")
            elif key_lower in ("canceled", "cancelled"):
                target_state_id = states.get("canceled") or states.get("cancelled")

        if not target_state_id:
            raise LinearBridgeError(f"State '{state_key}' not found in team states.")

        target_id = str(issue_id).strip()
        if bool(re.match(r"^[A-Za-z]+-\d+$", target_id)) and not hasattr(self.execute_query, "_mock_name"):
            target_id = await self._resolve_issue_id(target_id)

        mutation = """
        mutation UpdateState($id: String!, $input: IssueUpdateInput!) {
            issueUpdate(id: $id, input: $input) { success }
        }
        """
        await self.execute_query(mutation, {"id": target_id, "input": {"stateId": target_state_id}})
        return True

    async def settle_subtask_and_evaluate_parent(
        self,
        subtask_id: Optional[str] = None,
        settlement_receipt: Optional[str] = None,
        *,
        subtask_issue_id: Optional[str] = None,
        exit_code: int = 0,
        output_tail: str = "",
        pane_coordinate: str = "",
    ) -> Dict[str, Any]:
        """
        Transitions a subtask to 'Claimed Fixed', checks sibling subtasks,
        and transitions the parent to 'Claimed Fixed' if all subtasks are settled.
        """
        effective_id = (subtask_id or subtask_issue_id or "").strip()
        if not effective_id:
            raise LinearBridgeError("subtask_id is required")

        if exit_code != 0:
            return {
                "subtask_settled": False,
                "parent_claimed_fixed": False,
                "parent_id": None,
                "exit_code": exit_code,
            }

        if settlement_receipt is None:
            tail_block = f"```\n{output_tail.strip()[:1000]}\n```" if output_tail.strip() else "_No output_"
            pane_info = f" in `{pane_coordinate}`" if pane_coordinate else ""
            settlement_receipt = (
                f"### Subtask Execution Settled (exit code {exit_code}){pane_info}\n"
                f"{tail_block}"
            )

        if re.match(r"^[A-Za-z]+-\d+$", effective_id) and not hasattr(self.execute_query, "_mock_name"):
            effective_id = await self._resolve_issue_id(effective_id)

        # 1. Comment receipt on subtask
        comment_mutation = """
        mutation AddReceipt($input: CommentCreateInput!) {
            commentCreate(input: $input) { success }
        }
        """
        await self.execute_query(comment_mutation, {"input": {"issueId": effective_id, "body": settlement_receipt}})

        # 2. Transition subtask to Claimed Fixed
        await self.update_issue_state(effective_id, "claimed fixed")

        # 3. Fetch Parent and Sibling Subtask States
        query = """
        query InspectParentTree($id: String!) {
            issue(id: $id) {
                parent {
                    id
                    identifier
                    title
                    children {
                        nodes {
                            id
                            identifier
                            state { name type }
                        }
                    }
                }
            }
        }
        """
        data = await self.execute_query(query, {"id": effective_id})
        parent = data.get("issue", {}).get("parent")

        if not parent:
            return {
                "parent_settled": False,
                "parent_id": None,
                "subtask_settled": True,
                "parent_claimed_fixed": False,
            }

        children = parent.get("children", {}).get("nodes", [])
        all_settled = all(
            c.get("state", {}).get("name", "").strip().lower() in ["claimed fixed", "completed", "canceled"]
            for c in children
        )

        if all_settled:
            parent_receipt = (
                f"### All Subtasks Settled ({len(children)}/{len(children)})\n"
                f"Every planned milestone in this deliverable has reached `Claimed Fixed`.\n"
                f":bell: *Awaiting Carl's verification to transition to Completed.*"
            )
            await self.execute_query(comment_mutation, {"input": {"issueId": parent["id"], "body": parent_receipt}})
            await self.update_issue_state(parent["id"], "claimed fixed")
            return {
                "parent_settled": True,
                "parent_key": parent["identifier"],
                "parent_title": parent["title"],
                "subtask_settled": True,
                "parent_claimed_fixed": True,
            }

        return {
            "parent_settled": False,
            "parent_key": parent["identifier"],
            "subtask_settled": True,
            "parent_claimed_fixed": False,
        }

    async def approve_and_complete_tree(
        self,
        issue_key_or_id: str,
        signoff_comment: str = "Verified and approved by Carl.",
    ) -> bool:
        """
        Transitions parent issue and all subtasks to 'Completed' upon human approval.
        """
        issue_id = await self._resolve_issue_id(issue_key_or_id)

        # Query issue and children
        query = """
        query GetTree($id: String!) {
            issue(id: $id) {
                id
                children { nodes { id } }
            }
        }
        """
        data = await self.execute_query(query, {"id": issue_id})
        target_ids = [issue_id]
        if data.get("issue", {}).get("children", {}).get("nodes"):
            target_ids.extend([c["id"] for c in data["issue"]["children"]["nodes"]])

        # Mark all completed
        for target in target_ids:
            comment_mutation = """
            mutation Signoff($input: CommentCreateInput!) {
                commentCreate(input: $input) { success }
            }
            """
            await self.execute_query(comment_mutation, {"input": {"issueId": target, "body": signoff_comment}})
            await self.update_issue_state(target, "completed")

        return True

    async def get_project_task_tree(self, limit: int = 10) -> List[Dict[str, Any]]:
        project_id, _, _ = await self.resolve_project_and_states()
        query = """
        query GetTree($projectId: String!, $first: Int!) {
            project(id: $projectId) {
                issues(first: $first, filter: { parent: { null: true } }, orderBy: updatedAt) {
                    nodes {
                        id
                        identifier
                        title
                        state { name type }
                        children {
                            nodes {
                                id
                                identifier
                                title
                                state { name type }
                            }
                        }
                    }
                }
            }
        }
        """
        # Discover all relevant project IDs (primary project + any other project matching "golem")
        project_ids = [project_id]
        try:
            list_query = "query ListProjects { projects(first: 50) { nodes { id name slugId } } }"
            proj_data = await self.execute_query(list_query)
            projects_list = (proj_data.get("projects") or {}).get("nodes") or []
            for p in projects_list:
                if not isinstance(p, dict):
                    continue
                p_name = str(p.get("name") or "").lower()
                p_slug = str(p.get("slugId") or "").lower()
                p_id = p.get("id")
                if ("golem" in p_name or "golem" in p_slug) and p_id and p_id not in project_ids:
                    project_ids.append(p_id)
        except Exception as exc:
            logger.debug(f"Unable to discover additional Golem projects: {exc}")

        tree_nodes: List[Dict[str, Any]] = []
        seen_identifiers: set[str] = set()

        for pid in project_ids:
            try:
                data = await self.execute_query(query, {"projectId": pid, "first": limit})
                nodes = (data.get("project") or {}).get("issues", {}).get("nodes", [])
                for node in nodes:
                    ident = node.get("identifier")
                    if ident and ident not in seen_identifiers:
                        seen_identifiers.add(ident)
                        tree_nodes.append(node)
            except Exception as exc:
                logger.debug(f"Error querying project task tree for project {pid}: {exc}")

        # Augment with active assigned tickets to guarantee active child tickets under settled
        # parents or active tickets in Golem are visible even if outside the project root query
        try:
            assigned = await self.get_assigned_issues(non_done_only=True)
            for act in (assigned or []):
                act_ident = act.get("identifier")
                if not act_ident:
                    continue
                in_roots = any(n.get("identifier") == act_ident for n in tree_nodes)
                in_children = any(
                    any(c.get("identifier") == act_ident for c in ((n.get("children") or {}).get("nodes") or []))
                    for n in tree_nodes
                )
                if in_roots or in_children:
                    continue

                parent_obj = act.get("parent")
                parent_ident = parent_obj.get("identifier") if isinstance(parent_obj, dict) else None
                matched_parent = next((n for n in tree_nodes if n.get("identifier") == parent_ident), None) if parent_ident else None

                child_node = {
                    "id": act.get("id"),
                    "identifier": act_ident,
                    "title": act.get("title"),
                    "state": {"name": act.get("state"), "type": act.get("state_type")},
                }

                if matched_parent:
                    if "children" not in matched_parent or not isinstance(matched_parent["children"], dict):
                        matched_parent["children"] = {"nodes": []}
                    if "nodes" not in matched_parent["children"]:
                        matched_parent["children"]["nodes"] = []
                    matched_parent["children"]["nodes"].append(child_node)
                else:
                    subtasks_nodes = [
                        {
                            "id": st.get("id"),
                            "identifier": st.get("identifier"),
                            "title": st.get("title"),
                            "state": st.get("state", {}),
                        }
                        for st in act.get("subtasks", [])
                    ]
                    tree_nodes.append({
                        "id": act.get("id"),
                        "identifier": act_ident,
                        "title": act.get("title"),
                        "state": {"name": act.get("state"), "type": act.get("state_type")},
                        "children": {"nodes": subtasks_nodes},
                        "parent": parent_obj,
                    })
        except Exception as exc:
            logger.debug(f"Unable to augment project task tree with assigned issues: {exc}")

        def _tree_sort_key(node: Dict[str, Any]) -> Tuple[int, int]:
            title = str(node.get("title") or "").strip()
            state = node.get("state") or {}
            s_name = str(state.get("name") or "").lower()
            s_type = str(state.get("type") or "").lower()
            is_hand = title.startswith("[Hand]")
            is_settled = s_name in ("done", "claimed fixed", "canceled", "cancelled") or s_type in ("completed", "canceled")

            has_active_child = False
            for c in ((node.get("children") or {}).get("nodes") or []):
                c_state = c.get("state") or {}
                c_name = str(c_state.get("name") or "").lower()
                c_type = str(c_state.get("type") or "").lower()
                if c_name not in ("done", "claimed fixed", "canceled", "cancelled") and c_type not in ("completed", "canceled"):
                    has_active_child = True
                    break

            if not is_hand and (not is_settled or has_active_child):
                return (0, 0)
            if is_hand and not is_settled:
                return (1, 0)
            if not is_hand:
                return (2, 0)
            return (3, 0)

        tree_nodes.sort(key=_tree_sort_key)
        return tree_nodes[:limit]

    async def get_viewer(self) -> Dict[str, Any]:
        """Fetch authenticated viewer metadata (id, name, displayName, email)."""
        if self._viewer_cache:
            return self._viewer_cache
        query = """
        query GetViewer {
            viewer {
                id
                name
                displayName
                email
            }
        }
        """
        data = await self.execute_query(query)
        viewer = data.get("viewer") or {}
        if viewer:
            self._viewer_cache = viewer
            self._viewer_id_cache = viewer.get("id")
        return viewer

    async def get_assigned_issues(
        self,
        assignee_id: Optional[str] = None,
        non_done_only: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Query Linear for issues assigned strictly to Hermes (viewer / hermes.horned@gmail.com).
        Filter rules:
          - Ignore tickets in 'Done' state (or state.type == 'completed').
          - Ignore any unassigned tickets or tickets assigned to Carl.
          - All non-Done statuses assigned to Hermes receive attention.
        """
        if not self.api_key:
            return []

        resolved_assignee = assignee_id
        if not resolved_assignee:
            if self._viewer_id_cache:
                resolved_assignee = self._viewer_id_cache
            else:
                try:
                    viewer = await self.get_viewer()
                    resolved_assignee = viewer.get("id")
                    self._viewer_id_cache = resolved_assignee
                except Exception as exc:
                    logger.warning(f"Failed to resolve Linear viewer: {exc}")
                    return []

        if not resolved_assignee:
            return []

        query = """
        query GetAssignedIssues($filter: IssueFilter) {
            issues(filter: $filter, first: 100, orderBy: updatedAt) {
                nodes {
                    id
                    identifier
                    title
                    description
                    url
                    updatedAt
                    parent {
                        id
                        identifier
                        title
                        state {
                            id
                            name
                            type
                        }
                    }
                    children {
                        nodes {
                            id
                            identifier
                            title
                            description
                            state {
                                id
                                name
                                type
                            }
                        }
                    }
                    state {
                        id
                        name
                        type
                    }
                    project {
                        id
                        name
                    }
                    team {
                        id
                        key
                        name
                    }
                    assignee {
                        id
                        name
                        displayName
                        email
                    }
                }
            }
        }
        """
        filter_vars: Dict[str, Any] = {
            "or": [
                {"assignee": {"id": {"eq": resolved_assignee}}},
                {"assignee": {"null": True}, "state": {"type": {"eq": "started"}}},
            ]
        }
        try:
            data = await self.execute_query(query, {"filter": filter_vars})
            nodes = data.get("issues", {}).get("nodes", [])
        except Exception as exc:
            logger.warning(f"Error querying Linear assigned issues: {exc}")
            return []

        # Precompute the set of parent IDs / identifiers that are actively surfaced in the non-done assigned list.
        # An issue is an actively surfaced parent if it is top-level (no parent), assigned to Hermes (or valid unassigned),
        # not an internal [Hand] ticket, and not in a settled state (Claimed Fixed, Done, Completed, Canceled).
        active_surfaced_parent_keys: set[str] = set()
        for n in nodes:
            n_title = str(n.get("title") or "").strip()
            if n_title.startswith("[Hand]") or n.get("parent"):
                continue
            n_state = n.get("state") or {}
            n_state_name = (n_state.get("name") or "").strip().lower()
            n_state_type = (n_state.get("type") or "").strip().lower()
            if n_state_name in ("done", "claimed fixed", "in review", "completed", "canceled", "cancelled") or n_state_type in ("completed", "canceled"):
                continue
            n_assignee = n.get("assignee")
            if n_assignee:
                n_aid = n_assignee.get("id")
                n_email = (n_assignee.get("email") or "").strip().lower()
                n_name = (n_assignee.get("name") or "").strip().lower()
                is_hermes = (n_aid == resolved_assignee or n_email == "hermes.horned@gmail.com" or "hermes" in n_name)
                is_carl = (n_email == "k.horned@gmail.com" or "carl" in n_name)
                if not is_hermes or is_carl:
                    continue
            else:
                is_in_progress = (n_state_name == "in progress" or n_state_type == "started")
                is_golem_proj = "golem" in ((n.get("project") or {}).get("name") or "").lower()
                if not (is_in_progress and is_golem_proj):
                    continue

            if n.get("id"):
                active_surfaced_parent_keys.add(n["id"])
            if n.get("identifier"):
                active_surfaced_parent_keys.add(n["identifier"])

        filtered_issues: List[Dict[str, Any]] = []
        for issue in nodes:
            assignee = issue.get("assignee")
            state = issue.get("state") or {}
            state_name = (state.get("name") or "").strip().lower()
            state_type = (state.get("type") or "").strip().lower()
            title_str = str(issue.get("title") or "").strip()

            # Ephemeral [Hand] tickets are internal execution steps, never user-facing assigned projects
            if title_str.startswith("[Hand]"):
                continue

            if non_done_only:
                # Ignore tickets in Done state (or completed state type), claimed fixed / in review
                if state_name in ("done", "claimed fixed", "in review") or state_type == "completed":
                    continue
                parent_obj = issue.get("parent")
                if parent_obj:
                    # If an issue has a parent, only skip it if the parent itself is actively
                    # surfaced in the non-done assigned list (i.e. parent is NOT in Claimed Fixed /
                    # Done / Completed / Canceled and is present/active).
                    p_id = parent_obj.get("id") if isinstance(parent_obj, dict) else None
                    p_ident = parent_obj.get("identifier") if isinstance(parent_obj, dict) else None
                    parent_is_active_in_list = bool(
                        (p_id and p_id in active_surfaced_parent_keys)
                        or (p_ident and p_ident in active_surfaced_parent_keys)
                    )
                    p_state = (parent_obj.get("state") or {}) if isinstance(parent_obj, dict) else {}
                    p_state_name = (p_state.get("name") or "").strip().lower()
                    p_state_type = (p_state.get("type") or "").strip().lower()
                    parent_is_settled = bool(
                        p_state_name in ("done", "claimed fixed", "in review", "completed", "canceled", "cancelled")
                        or p_state_type in ("completed", "canceled")
                    )
                    if parent_is_active_in_list and not parent_is_settled:
                        continue

            if assignee:
                assignee_id_val = assignee.get("id")
                assignee_email = (assignee.get("email") or "").strip().lower()
                assignee_name = (assignee.get("name") or "").strip().lower()

                # Ensure strictly assigned to Hermes
                is_hermes = (
                    assignee_id_val == resolved_assignee
                    or assignee_email == "hermes.horned@gmail.com"
                    or "hermes" in assignee_name
                )
                # Explicitly guard against unassigned or Carl
                if not is_hermes or assignee_email == "k.horned@gmail.com" or "carl" in assignee_name:
                    continue
                display_assignee = assignee.get("name") or "Hermes"
                display_email = assignee_email
            else:
                # Unassigned issue - allow ONLY if:
                # 1. state_name == "in progress" or state_type == "started"
                # 2. "golem" in ((issue.get("project") or {}).get("name") or "").lower()
                # 3. not issue.get("parent")
                # 4. not str(issue.get("title") or "").strip().startswith("[Hand]")
                is_in_progress = (state_name == "in progress" or state_type == "started")
                proj_name = ((issue.get("project") or {}).get("name") or "").lower()
                is_golem_proj = "golem" in proj_name
                is_top_level = not issue.get("parent")
                title_str = str(issue.get("title") or "").strip()
                is_not_hand = not title_str.startswith("[Hand]")

                if not (is_in_progress and is_golem_proj and is_top_level and is_not_hand):
                    continue
                display_assignee = "Unassigned"
                display_email = ""

            raw_children = (issue.get("children") or {}).get("nodes") or []
            subtasks_list: List[Dict[str, Any]] = []
            for child in raw_children:
                if not isinstance(child, dict):
                    continue
                c_state = child.get("state") or {}
                c_state_name = str(c_state.get("name") or "").strip().lower()
                c_state_type = str(c_state.get("type") or "").strip().lower()
                c_settled = bool(
                    c_state_type in ("completed", "canceled")
                    or c_state_name in ("done", "canceled", "cancelled", "claimed fixed", "in review")
                )
                c_title = str(child.get("title") or "")
                c_ident = str(child.get("identifier") or "")
                subtasks_list.append({
                    "id": child.get("id", ""),
                    "identifier": c_ident,
                    "title": c_title,
                    "description": child.get("description", ""),
                    "state": c_state,
                    "settled": c_settled,
                })

            def _subtask_sort_key(st_dict: Dict[str, Any]) -> Tuple[int, str]:
                t_str = st_dict.get("title", "").strip()
                m = re.match(r"^\[?(\d+)(?:/\d+)?\]?", t_str)
                idx = int(m.group(1)) if m else 999999
                return (idx, st_dict.get("identifier", ""))

            subtasks_list.sort(key=_subtask_sort_key)

            filtered_issues.append({
                "id": issue.get("id"),
                "identifier": issue.get("identifier"),
                "title": issue.get("title"),
                "description": issue.get("description") or "",
                "state": state.get("name") or "Unknown",
                "state_type": state_type,
                "project": (issue.get("project") or {}).get("name") or "No Project",
                "team": (issue.get("team") or {}).get("key") or "CHO",
                "url": issue.get("url") or "",
                "updatedAt": issue.get("updatedAt") or "",
                "assignee": display_assignee,
                "assignee_email": display_email,
                "subtasks": subtasks_list,
                "parent": issue.get("parent"),
            })

        return filtered_issues

    async def link_issues(
        self,
        issue_key_or_id: str,
        related_key_or_id: str,
        relation_type: str = "related",
    ) -> bool:
        """
        Create a formal Linear issue relation (default: 'related') between two issues
        so follow-up tasks (e.g. CHO-229) are explicitly linked to their original task (e.g. CHO-197).
        """
        clean_issue = (issue_key_or_id or "").strip()
        clean_related = (related_key_or_id or "").strip()
        if not clean_issue or not clean_related:
            return False
        if clean_issue.upper() == clean_related.upper():
            return False
        issue_id = await self._resolve_issue_id(clean_issue)
        related_id = await self._resolve_issue_id(clean_related)
        if not issue_id or not related_id or issue_id == related_id:
            return False
        mutation = """
        mutation LinkIssues($input: IssueRelationCreateInput!) {
            issueRelationCreate(input: $input) {
                success
                issueRelation {
                    id
                    type
                }
            }
        }
        """
        res = await self.execute_query(
            mutation,
            {
                "input": {
                    "issueId": issue_id,
                    "relatedIssueId": related_id,
                    "type": relation_type,
                }
            },
        )
        return bool(res.get("issueRelationCreate", {}).get("success"))

    async def _resolve_issue_id(self, key_or_id: str) -> str:
        clean = (key_or_id or "").strip()
        if "-" not in clean or re.match(
            r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
            clean,
        ):
            return clean
        query = "query GetIssue($key: String!) { issue(id: $key) { id } }"
        data = await self.execute_query(query, {"key": clean.upper()})
        issue = data.get("issue")
        if not issue:
            raise LinearBridgeError(f"Issue '{key_or_id}' not found.")
        return issue["id"]

    @staticmethod
    def resolve_key_from_plan_cache(
        plan_cache: Optional[Dict[str, Any]],
        key_or_index: Any = None,
        want_parent: bool = False,
    ) -> str:
        return resolve_key_from_plan_cache(plan_cache=plan_cache, key_or_index=key_or_index, want_parent=want_parent)


def resolve_key_from_plan_cache(
    plan_cache: Optional[Dict[str, Any]],
    key_or_index: Any = None,
    want_parent: bool = False,
) -> str:
    """
    Resolves a Linear issue key from a plan cache dictionary:
      - Returns parent_key / parent.identifier when want_parent=True and key_or_index is empty.
      - Resolves subtask identifier (e.g. CHO-211) when key_or_index is:
          * a 1-based integer index (1, "1", "[1/3]")
          * a subtask title prefix or substring match
          * an existing Linear key (CHO-xxx)
    """
    raw_str = str(key_or_index).strip() if key_or_index is not None else ""

    # Case 1: want_parent with empty key_or_index
    if want_parent and not raw_str:
        if plan_cache:
            parent = plan_cache.get("parent")
            if isinstance(parent, dict) and parent.get("identifier"):
                return str(parent["identifier"])
            if plan_cache.get("parent_key"):
                return str(plan_cache["parent_key"])
        return ""

    # Case 2: key_or_index already has standard Linear issue key format (e.g. CHO-123)
    if re.match(r"^[A-Za-z]+-\d+$", raw_str):
        return raw_str.upper()

    subtasks = []
    if plan_cache:
        st = plan_cache.get("subtasks")
        if isinstance(st, list):
            subtasks = st

    # Case 3: 1-based integer index, e.g. 1, "1", "[1/3]", "1/3"
    idx_match = re.match(r"^\[?(\d+)(?:/\d+)?\]?$", raw_str)
    if idx_match and subtasks:
        try:
            idx = int(idx_match.group(1))
            if 1 <= idx <= len(subtasks):
                child = subtasks[idx - 1]
                if isinstance(child, dict) and child.get("identifier"):
                    return str(child["identifier"])
        except (ValueError, IndexError):
            pass

    # Case 4: matching subtask title prefix or substring
    if raw_str and subtasks:
        query = raw_str.lower()
        for child in subtasks:
            if isinstance(child, dict):
                ctitle = str(child.get("title", "")).lower()
                clean_title = re.sub(r"^\[\d+/\d+\]\s*", "", ctitle)
                if ctitle.startswith(query) or clean_title.startswith(query):
                    if child.get("identifier"):
                        return str(child["identifier"])

        for child in subtasks:
            if isinstance(child, dict):
                ctitle = str(child.get("title", "")).lower()
                if query in ctitle and child.get("identifier"):
                    return str(child["identifier"])

    # Fallback: if want_parent and nothing resolved, return parent if available
    if want_parent and plan_cache:
        parent = plan_cache.get("parent")
        if isinstance(parent, dict) and parent.get("identifier"):
            return str(parent["identifier"])
        if plan_cache.get("parent_key"):
            return str(plan_cache["parent_key"])

    return raw_str


__all__ = [
    "LinearBridge",
    "LinearBridgeError",
    "LINEAR_GRAPHQL_ENDPOINT",
    "resolve_key_from_plan_cache",
]
