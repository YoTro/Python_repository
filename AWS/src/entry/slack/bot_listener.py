from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import threading

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# Ensure project root is in path correctly
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.core.utils.config_helper import ConfigHelper
from src.entry.slack.commands import CommandDispatcher
from src.jobs.interactions import InteractionRegistry

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("slack-bot")

# Global Registry
dispatcher: CommandDispatcher | None = None
global_bg_loop: asyncio.AbstractEventLoop | None = None

# Slack auto-wraps URLs in message text as `<https://example.com/path|display text>`
# (or bare `<https://example.com/path>` with no display text). Strip that mrkdwn
# envelope down to the raw URL before any command tries to regex a URL out of the text.
_SLACK_LINK_RE = re.compile(r"<(https?://[^|>]+)(?:\|[^>]*)?>")


def _unwrap_slack_links(text: str) -> str:
    return _SLACK_LINK_RE.sub(r"\1", text)


def handle_message_event(event: dict) -> None:
    try:
        if dispatcher is None:
            return
        # Skip the bot's own messages and other subtypes (edits, joins, etc.) —
        # unlike Feishu's callback protocol, Slack's "message" event fires for
        # everything in a subscribed conversation, including the bot's own posts.
        if event.get("bot_id") or event.get("subtype"):
            return

        text = _unwrap_slack_links(event.get("text", ""))
        chat_id = event.get("channel")
        if not chat_id:
            return
        logger.info(f"Message from {chat_id}: {text}")
        if not dispatcher.dispatch(text, chat_id):
            logger.info("Message ignored.")
    except Exception as e:
        logger.error(f"Error: {e}")


def _resolve_dm_channel(client, body: dict) -> str | None:
    """
    Resolve the channel to reply to for a slash command.

    Slack slash-command payloads include `channel_id`, but for DMs (D-prefixed)
    that id has been observed to 404 with channel_not_found on chat.postMessage
    in some installs (stale id relative to the token's app install). Re-resolving
    via conversations.open(users=user_id) — Slack's documented way to get a
    guaranteed-valid DM channel id — sidesteps that. Regular channels (C-prefixed)
    are returned as-is since the bot is already a member and posting works directly.
    """
    channel_id = body.get("channel_id")
    if channel_id and not channel_id.startswith("D"):
        return channel_id

    user_id = body.get("user_id")
    if not user_id:
        return channel_id

    try:
        resp = client.conversations_open(users=user_id)
        return resp["channel"]["id"]
    except Exception as e:
        logger.error(f"conversations_open failed for user {user_id}: {e}")
        return channel_id


def handle_slash_command(command: str, chat_id: str) -> None:
    try:
        if dispatcher is None:
            return
        logger.info(f"Slash command {command} from {chat_id}")
        if not dispatcher.dispatch_slash(command, chat_id):
            logger.warning(f"Unrecognized slash command: {command}")
    except Exception as e:
        logger.error(f"Error handling slash command {command}: {e}")


def handle_block_action(action: dict) -> None:
    """Handle interactive Block Kit button clicks (mirrors Feishu card-action-trigger)."""
    try:
        raw_value = action.get("value")
        if not raw_value:
            logger.warning(f"Block action triggered but no 'value' found: {action}")
            return

        try:
            action_value = json.loads(raw_value)
        except (TypeError, ValueError):
            logger.warning(f"Block action value is not JSON: {raw_value!r}")
            return

        action_name = action_value.get("action")
        if not action_name:
            logger.warning(f"Block action triggered but no 'action' key in value: {action_value}")
            return

        logger.info(f"Received block action: {action_name}")

        if global_bg_loop:

            async def _handle_and_log():
                try:
                    result = await InteractionRegistry.handle(action_name, action_value)
                    logger.info(f"Interaction result: {result}")
                except Exception as err:
                    logger.error(f"Error handling interaction {action_name}: {err}")

            asyncio.run_coroutine_threadsafe(_handle_and_log(), global_bg_loop)
        else:
            logger.error("Cannot handle block action: Background event loop not ready.")

    except Exception as e:
        logger.error(f"Error processing block action: {e}", exc_info=True)


def build_app(bot_config: dict) -> App:
    app = App(
        token=bot_config["bot_token"], signing_secret=bot_config.get("signing_secret") or None
    )

    @app.event("message")
    def _on_message(event, ack=None):  # noqa: ANN001 — slack_bolt injects these
        handle_message_event(event)

    @app.event("app_home_opened")
    def _on_app_home_opened(body):  # noqa: ANN001
        # No Home tab view is published; this only silences Bolt's "unhandled
        # request" 404 log when a user opens the bot's profile/Home tab.
        logger.debug(f"app_home_opened: {body}")

    @app.command("/list")
    def _on_list(ack, body, client):  # noqa: ANN001
        ack()
        chat_id = _resolve_dm_channel(client, body)
        handle_slash_command("/list", chat_id)

    @app.command("/top")
    def _on_top(ack, body, client):  # noqa: ANN001
        ack()
        chat_id = _resolve_dm_channel(client, body)
        handle_slash_command("/top", chat_id)

    @app.action(re.compile(".*"))  # matches any action_id
    def _on_action(ack, body):  # noqa: ANN001
        ack()
        actions = body.get("actions") or []
        if actions:
            handle_block_action(actions[0])

    return app


def main():
    global dispatcher
    global global_bg_loop
    parser = argparse.ArgumentParser(description="Slack Bot Listener")
    parser.add_argument("--bot", type=str, help="Name of the bot", default="toryunbot")
    args = parser.parse_args()

    bot_config = ConfigHelper.get_slack_bot(args.bot)
    if not bot_config:
        logger.error(
            f"Bot '{args.bot}' not configured. Set SLACK_{args.bot.upper()}_BOT_TOKEN in .env"
        )
        sys.exit(1)
    if not bot_config.get("app_token"):
        logger.error(
            f"Bot '{args.bot}' has no App-Level Token. Set SLACK_{args.bot.upper()}_APP_TOKEN "
            "(xapp-..., needs connections:write scope) in .env — required for Socket Mode."
        )
        sys.exit(1)

    logger.info(f"Starting Socket Mode client for bot: {args.bot}")

    bg_loop: asyncio.AbstractEventLoop | None = None
    loop_ready = threading.Event()

    def bg_tasks_thread():
        nonlocal bg_loop
        bg_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(bg_loop)
        loop_ready.set()
        bg_loop.run_forever()

    threading.Thread(target=bg_tasks_thread, daemon=True).start()

    if loop_ready.wait(timeout=5):
        global_bg_loop = bg_loop
        dispatcher = CommandDispatcher(bot_name=args.bot, loop=bg_loop)
    else:
        sys.exit(1)

    app = build_app(bot_config)
    handler = SocketModeHandler(app, bot_config["app_token"])
    handler.start()


if __name__ == "__main__":
    main()
