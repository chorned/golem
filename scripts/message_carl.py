#!/usr/bin/env python3
"""
scripts/message_carl.py
Command-line tool for Golem to message Carl on demand in Slack, iMessage, or both.

Usage examples:
  # Message on both Slack and iMessage
  python scripts/message_carl.py -m "Hey Carl, docker build finished. Need your review on the PR."
  
  # Message on Slack only
  python scripts/message_carl.py -t slack -m "Hey Carl, quick question about the database migration."
  
  # Message on iMessage only
  python scripts/message_carl.py -t imessage -m "Hey Carl, server reboot is complete."

  # Continue an existing Slack thread
  python scripts/message_carl.py -t slack --slack-thread-ts 1740000000.123456 -m "Build passed!"
"""

import argparse
import asyncio
import json
import os
import sys

# Ensure repository root is on sys.path
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from golem.notify import send_on_demand_message


def parse_args():
    parser = argparse.ArgumentParser(
        description="Send an on-demand message to Carl on Slack, iMessage, or both with thread continuity."
    )
    parser.add_argument(
        "-m", "--message",
        required=True,
        help="Message content to deliver to Carl."
    )
    parser.add_argument(
        "-t", "--target",
        choices=["slack", "imessage", "both"],
        default="both",
        help="Target platform (default: 'both')."
    )
    parser.add_argument(
        "--slack-channel",
        default=None,
        help="Optional custom Slack channel ID (defaults to Carl's DM: D0C2J1HKNN7)."
    )
    parser.add_argument(
        "--slack-thread-ts",
        default=None,
        help="Optional Slack thread_ts to continue an existing thread."
    )
    parser.add_argument(
        "--imessage-guid",
        default=None,
        help="Optional custom iMessage chat GUID (defaults to iMessage;-;k.horned@gmail.com)."
    )
    parser.add_argument(
        "--imessage-reply-to",
        default=None,
        help="Optional iMessage reply-to message GUID."
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output raw JSON results."
    )
    return parser.parse_args()


async def main():
    args = parse_args()
    res = await send_on_demand_message(
        text=args.message,
        target=args.target,
        slack_channel=args.slack_channel,
        slack_thread_ts=args.slack_thread_ts,
        imessage_chat_guid=args.imessage_guid,
        imessage_reply_to_guid=args.imessage_reply_to,
    )

    if args.json:
        print(json.dumps(res, indent=2))
    else:
        if args.target in ("slack", "both"):
            s_res = res.get("slack", {})
            if s_res.get("ok"):
                print(f"✅ Slack message delivered to {s_res.get('channel')} (ts: {s_res.get('ts')})")
            else:
                print(f"❌ Slack message failed: {s_res.get('error')}")

        if args.target in ("imessage", "both"):
            i_res = res.get("imessage", {})
            if i_res.get("ok"):
                print(f"✅ iMessage delivered to {i_res.get('chat_guid')} (guid: {i_res.get('guid')})")
            else:
                print(f"❌ iMessage failed: {i_res.get('error')}")

    sys.exit(0 if res.get("ok") else 1)


if __name__ == "__main__":
    asyncio.run(main())
