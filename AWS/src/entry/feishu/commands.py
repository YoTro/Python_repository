from __future__ import annotations
from abc import ABC, abstractmethod
import re
import logging
import asyncio
from typing import Optional
from src.core.utils.config_helper import ConfigHelper
from src.entry.feishu.client import FeishuClient
from src.core.utils.cookie_helper import AmazonCookieHelper
from src.core.telemetry.tracker import TimeEstimator

logger = logging.getLogger(__name__)

class BotCommand(ABC):
    """Abstract Strategy for handling Feishu Bot commands."""
    
    def __init__(self, bot_name: str = "amazon_bot", loop: Optional[asyncio.AbstractEventLoop] = None):
        self.bot_name = bot_name
        self.loop = loop or asyncio.get_event_loop()

    @abstractmethod
    def match(self, text: str) -> bool: pass
        
    @abstractmethod
    def execute(self, text: str, chat_id: str): pass

class RefreshCookieCommand(BotCommand):
    def match(self, text: str) -> bool:
        return "更新亚马逊" in text and "Cookies" in text

    def execute(self, text: str, chat_id: str):
        logger.info(f"Manual cookie refresh triggered: {chat_id}")
        feishu_client = FeishuClient(bot_name=self.bot_name)
        feishu_client.send_text_message("chat_id", chat_id, "🔄 收到！正在启动有头浏览器刷新亚马逊 Cookies...")

        async def _do_refresh():
            try:
                # Import here to avoid circular imports if any
                from src.core.utils.cookie_helper import AmazonCookieHelper
                helper = AmazonCookieHelper(headless=False)
                # Run the blocking fetch_fresh_cookies in a separate thread
                await asyncio.to_thread(helper.fetch_fresh_cookies)
                feishu_client.send_text_message("chat_id", chat_id, "✅ 亚马逊 Cookies 更新成功！")
                
                bot_cfg = ConfigHelper.get_feishu_bot(self.bot_name)
                webhook_url = bot_cfg["webhook_url"] if bot_cfg else ""
                if webhook_url:
                    feishu_client.send_webhook_message(webhook_url, "✅ 亚马逊 Cookies 手动刷新任务已完成。")
            except Exception as e:
                logger.error(f"Cookie refresh failed: {e}")
                feishu_client.send_text_message("chat_id", chat_id, f"❌ Cookies 更新失败: {str(e)}")

        # Execute in background task
        asyncio.run_coroutine_threadsafe(_do_refresh(), self.loop)

class ExtractBSRCommand(BotCommand):
    def __init__(self, bot_name: str = "amazon_bot", loop: Optional[asyncio.AbstractEventLoop] = None):
        super().__init__(bot_name, loop)
        self.url_map = {
            "Electronics": "https://www.amazon.com/Best-Sellers-Electronics/zgbs/electronics/",
            "Camera": "https://www.amazon.com/Best-Sellers-Camera-Photo/zgbs/camera-photo/",
            "Software": "https://www.amazon.com/best-sellers-software/zgbs/software/"
        }

    def match(self, text: str) -> bool:
        return bool(re.search(r"获取\s*(.*?)\s*BSR", text, re.IGNORECASE))

    def execute(self, text: str, chat_id: str):
        match = re.search(r"获取\s*(.*?)\s*BSR", text, re.IGNORECASE)
        category = match.group(1).strip()
        target_url = self.url_map.get(category)
        
        feishu_client = FeishuClient(bot_name=self.bot_name)
        if target_url:
            eta = TimeEstimator.estimate_workflow("amazon_bsr", params={"category": category})
            feishu_client.send_text_message("chat_id", chat_id, f"🚀 开始抓取 {category} BSR 数据...\n⏱️ 预计耗时: {eta}")
            
            bot_cfg = ConfigHelper.get_feishu_bot(self.bot_name)
            user_token = bot_cfg["user_access_token"] if bot_cfg else ""
            webhook_url = bot_cfg["webhook_url"] if bot_cfg else ""
            
            async def _dispatch_job():
                from src.gateway import APIGateway
                import src.workflows.definitions.amazon_bsr
                
                params = {"amazon_url": target_url, "category": category}
                job_id = APIGateway.dispatch_feishu_command(
                    workflow_name="amazon_bsr",
                    params=params,
                    chat_id=chat_id,
                    bot_name=self.bot_name
                )
                logger.info(f"Feishu command routed to Gateway, job_id={job_id}, bot_name={self.bot_name}")

            asyncio.run_coroutine_threadsafe(_dispatch_job(), self.loop)
        else:
            feishu_client.send_text_message("chat_id", chat_id, f"❌ 未知类目: {category}")

class AnalyzeCategoryMonopolyCommand(BotCommand):
    def match(self, text: str) -> bool:
        return "分析垄断度" in text or "分析类目垄断度" in text

    def execute(self, text: str, chat_id: str):
        # Extract URL if present
        match = re.search(r"(https?://www\.amazon\.com[^\s]+)", text)
        url = match.group(1) if match else None
        
        feishu_client = FeishuClient(bot_name=self.bot_name)
        
        if not url:
            feishu_client.send_text_message("chat_id", chat_id, "❌ 请提供要分析的 Amazon Best Sellers 类目 URL。格式：分析垄断度 [URL]")
            return
            
        eta = TimeEstimator.estimate_workflow("category_monopoly_analysis", params={})
        feishu_client.send_text_message("chat_id", chat_id, f"📊 开始分析该类目垄断度...\n⏱️ 预计耗时: {eta}，由于需要抓取大量 ASIN 数据，请耐心等待。")
        
        async def _dispatch_job():
            try:
                from src.gateway import APIGateway
                import src.workflows.definitions.category_monopoly_analysis
                
                job_id = APIGateway.dispatch_feishu_command(
                    workflow_name="category_monopoly_analysis",
                    params={"url": url},
                    chat_id=chat_id,
                    bot_name=self.bot_name
                )
                logger.info(f"Monopoly analysis routed to Gateway, job_id={job_id}, bot_name={self.bot_name}")
            except Exception as e:
                logger.error(f"Monopoly analysis dispatch failed: {e}")
                feishu_client.send_text_message("chat_id", chat_id, f"❌ 分析任务启动失败: {e}")

        asyncio.run_coroutine_threadsafe(_dispatch_job(), self.loop)

class AgentExploreCommand(BotCommand):

    def match(self, text: str) -> bool:
        return True

    def execute(self, text: str, chat_id: str):
        logger.info(f"Agent fallback triggered for: {text}")
        feishu_client = FeishuClient(bot_name=self.bot_name)
        eta = TimeEstimator.estimate_agent()
        feishu_client.send_text_message("chat_id", chat_id, f"🤖 收到！正在召唤 MCP Agent 深度分析...\n⏱️ 预计耗时: {eta}")
        
        async def _dispatch_agent():
            try:
                from src.gateway import APIGateway
                job_id = APIGateway.dispatch_feishu_explore(intent=text, chat_id=chat_id, bot_name=self.bot_name)
                logger.info(f"Feishu explore command routed to Gateway, job_id={job_id}, bot_name={self.bot_name}")
            except Exception as e:
                logger.error(f"Agent dispatch failed: {e}")
                feishu_client.send_text_message("chat_id", chat_id, f"❌ Agent 调度失败: {e}")

        asyncio.run_coroutine_threadsafe(_dispatch_agent(), self.loop)

class CommandDispatcher:
    def __init__(self, bot_name: str = "amazon_bot", loop: Optional[asyncio.AbstractEventLoop] = None):
        self.commands = [
            RefreshCookieCommand(bot_name, loop), 
            ExtractBSRCommand(bot_name, loop), 
            AnalyzeCategoryMonopolyCommand(bot_name, loop),
            AgentExploreCommand(bot_name, loop)
        ]
        
    def dispatch(self, text: str, chat_id: str) -> bool:
        for cmd in self.commands:
            if cmd.match(text):
                cmd.execute(text, chat_id); return True
        return False
