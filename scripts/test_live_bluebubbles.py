#!/usr/bin/env python3
"""
scripts/test_live_bluebubbles.py
Integration probe against the live BlueBubbles server at http://127.0.0.1:1234.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

import asyncio
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

load_dotenv(REPO_ROOT / ".env")

from bluebubbles_transport import BlueBubblesTransport

async def main():
    url = os.environ.get("BLUEBUBBLES_URL", "http://127.0.0.1:1234")
    pw = os.environ.get("BLUEBUBBLES_PASSWORD", "!Fulurban88")
    allowed = [h.strip() for h in os.environ.get("BLUEBUBBLES_ALLOWED_HANDLES", "k.horned@gmail.com").split(",") if h.strip()]

    print(f"Connecting to BlueBubbles server at {url}...")
    transport = BlueBubblesTransport(
        server_url=url,
        password=pw,
        allowed_handles=allowed,
    )

    is_papi = await transport.is_private_api_available()
    print(f"is_private_api_available() -> {is_papi}")

    session = await transport._get_session()
    info_url = transport._api_url("/api/v1/server/info")
    async with session.get(info_url, timeout=2.5) as resp:
        print(f"GET /api/v1/server/info -> status {resp.status}")
        data = await resp.json()
        server_data = data.get("data", {})
        print(f"Server version: {server_data.get('server_version')}")
        print(f"Private API enabled: {server_data.get('private_api')}")
        print(f"Helper connected: {server_data.get('helper_connected')}")

    # Query chats via POST /api/v1/chat/query
    chats_url = transport._api_url("/api/v1/chat/query")
    async with session.post(chats_url, json={"limit": 5}, timeout=2.5) as resp:
        print(f"POST /api/v1/chat/query -> status {resp.status}")
        if resp.status == 200:
            chats_data = await resp.json()
            chats = chats_data.get("data", [])
            print(f"Discovered {len(chats)} existing chats on server.")
            if chats:
                sample_chat = chats[0]
                chat_guid = sample_chat.get("guid")
                print(f"Testing Private API on sample chat: {chat_guid}...")
                
                # Test set_chat_read
                await transport.set_chat_read(chat_guid)
                print("set_chat_read() executed cleanly.")

                # Test typing indicator on and off
                await transport.set_typing_indicator(chat_guid, True)
                print("set_typing_indicator(True) executed cleanly.")
                await asyncio.sleep(0.5)
                await transport.set_typing_indicator(chat_guid, False)
                print("set_typing_indicator(False) executed cleanly.")

    await transport.stop()
    print("Live probe completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())
