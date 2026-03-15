from __future__ import annotations
import lark_oapi as lark
from lark_oapi.api.im.v1 import *
import json
import logging
import asyncio
import os
import sys
import argparse
import threading
from typing import Optional

# Ensure project root is in path correctly
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.core.utils.config_helper import ConfigHelper
from src.entry.feishu.commands import CommandDispatcher

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger("feishu-bot")

# Global Registry
dispatcher: Optional[CommandDispatcher] = None

def do_p2_im_message_receive_v1(data: P2ImMessageReceiveV1) -> None:
    try:
        if dispatcher is None: return
        content_str = data.event.message.content
        content_json = json.loads(content_str)
        text = content_json.get("text", "")
        chat_id = data.event.message.chat_id
        logger.info(f"Message from {chat_id}: {text}")
        if not dispatcher.dispatch(text, chat_id):
            logger.info("Message ignored.")
    except Exception as e:
        logger.error(f"Error: {e}")

event_handler = lark.EventDispatcherHandler.builder("", "") \
    .register_p2_im_message_receive_v1(do_p2_im_message_receive_v1) \
    .build()

def main():
    global dispatcher
    parser = argparse.ArgumentParser(description="Feishu Bot Listener")
    parser.add_argument("--bot", type=str, help="Name of the bot", default="amazon_bot")
    args = parser.parse_args()

    bot_config = ConfigHelper.get_feishu_bot(args.bot)
    if not bot_config:
        logger.error(f"Bot '{args.bot}' not configured. Set FEISHU_{args.bot.upper()}_APP_ID in .env")
        sys.exit(1)

    app_id = bot_config["app_id"]
    app_secret = bot_config["app_secret"]
    logger.info(f"Starting WebSocket client for App: {app_id}")
    
    bg_loop: Optional[asyncio.AbstractEventLoop] = None
    loop_ready = threading.Event()

    def bg_tasks_thread():
        nonlocal bg_loop
        bg_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(bg_loop)
        loop_ready.set()
        bg_loop.run_forever()

    threading.Thread(target=bg_tasks_thread, daemon=True).start()

    if loop_ready.wait(timeout=5):
        dispatcher = CommandDispatcher(bot_name=args.bot, loop=bg_loop)
    else:
        sys.exit(1)

    client = lark.ws.Client(app_id, app_secret, event_handler=event_handler, log_level=lark.LogLevel.INFO)
    client.start()

if __name__ == "__main__":
    main()
