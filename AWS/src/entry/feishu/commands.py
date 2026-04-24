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
                from src.core.utils.cookie_helper import AmazonCookieHelper
                helper = AmazonCookieHelper(headless=False)
                await asyncio.to_thread(helper.fetch_fresh_cookies, True)
                feishu_client.send_card_message("chat_id", chat_id, "✅ 亚马逊 Cookies 更新成功！")
            except Exception as e:
                logger.error(f"Cookie refresh failed: {e}")
                feishu_client.send_card_message("chat_id", chat_id, f"❌ Cookies 更新失败: {str(e)}")

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

class ResumeJobCommand(BotCommand):
    """
    Handles '恢复任务 <job_id>' messages.

    Full resume flow:
      1. Parse job_id from the message.
      2. Load checkpoint — confirms the job exists and retrieves workflow_name + params.
      3. Rebuild FeishuCallback pointing back to the same chat_id.
      4. Call manager.resume_from_checkpoint() — engine skips completed steps automatically.
    """

    _PATTERN = re.compile(r"恢复任务\s+([0-9a-f]{8})", re.IGNORECASE)

    def match(self, text: str) -> bool:
        return bool(self._PATTERN.search(text))

    def execute(self, text: str, chat_id: str):
        match = self._PATTERN.search(text)
        job_id = match.group(1)

        feishu_client = FeishuClient(bot_name=self.bot_name)

        async def _do_resume():
            try:
                from src.jobs.checkpoint import CheckpointManager
                from src.jobs.manager import get_job_manager
                from src.jobs.callbacks.feishu import FeishuCallback
                from src.core.utils.config_helper import ConfigHelper

                checkpoint = CheckpointManager().load(job_id)
                if not checkpoint:
                    feishu_client.send_text_message(
                        "chat_id", chat_id,
                        f"❌ 未找到任务 `{job_id}` 的断点，无法恢复。"
                    )
                    return

                bot_cfg = ConfigHelper.get_feishu_bot(self.bot_name)
                callback = FeishuCallback(
                    chat_id=chat_id,
                    bot_name=self.bot_name,
                    user_token=bot_cfg.get("user_access_token") if bot_cfg else None,
                    webhook_url=bot_cfg.get("webhook_url") if bot_cfg else None,
                )

                feishu_client.send_text_message(
                    "chat_id", chat_id,
                    f"▶️ 正在从断点恢复任务 `{job_id}`（已完成步骤: {checkpoint.step_name}）…"
                )

                get_job_manager().resume_from_checkpoint(job_id=job_id, callback=callback)

            except Exception as e:
                logger.error(f"Resume job failed: {e}")
                feishu_client.send_text_message("chat_id", chat_id, f"❌ 恢复任务失败: {e}")

        asyncio.run_coroutine_threadsafe(_do_resume(), self.loop)


class ProductScreeningCommand(BotCommand):
    """
    Handles '产品筛选 <关键词>' messages.

    Dispatches the product_screening workflow which runs a multi-stage funnel:
      price/rating → competition → promo risk → profitability → compliance → ad ratio → LLM synthesis
    """

    _PATTERN = re.compile(r"产品筛选\s+(.+)", re.IGNORECASE)

    def match(self, text: str) -> bool:
        return bool(self._PATTERN.search(text))

    def execute(self, text: str, chat_id: str):
        match = self._PATTERN.search(text)
        keyword = match.group(1).strip()

        feishu_client = FeishuClient(bot_name=self.bot_name)
        eta = TimeEstimator.estimate_workflow("product_screening", params={"keyword": keyword})
        feishu_client.send_text_message(
            "chat_id", chat_id,
            f"🔍 开始筛选「{keyword}」类目产品...\n⏱️ 预计耗时: {eta}\n"
            "将依次执行价格/评分 → 利润 → 合规 → 广告流量多维过滤，请耐心等待。"
        )

        async def _dispatch_job():
            try:
                from src.gateway import APIGateway
                import src.workflows.definitions.product_screening

                job_id = APIGateway.dispatch_feishu_command(
                    workflow_name="product_screening",
                    params={"keyword": keyword},
                    chat_id=chat_id,
                    bot_name=self.bot_name
                )
                logger.info(f"Product screening routed to Gateway, job_id={job_id}, bot_name={self.bot_name}")
            except Exception as e:
                logger.error(f"Product screening dispatch failed: {e}")
                feishu_client.send_text_message("chat_id", chat_id, f"❌ 产品筛选任务启动失败: {e}")

        asyncio.run_coroutine_threadsafe(_dispatch_job(), self.loop)


class AdDiagnosisCommand(BotCommand):
    """
    Handles '广告诊断 <ASIN>' messages.

    Dispatches the ad_diagnosis workflow which fetches keyword/placement/change-history
    data, correlates changes with performance shifts, and produces an LLM report.
    """

    _PATTERN = re.compile(r"广告诊断\s+([A-Z0-9]{10})", re.IGNORECASE)

    def match(self, text: str) -> bool:
        return bool(self._PATTERN.search(text))

    def execute(self, text: str, chat_id: str):
        match = self._PATTERN.search(text)
        asin = match.group(1).upper()

        feishu_client = FeishuClient(bot_name=self.bot_name)
        eta = TimeEstimator.estimate_workflow("ad_diagnosis", params={"asin": asin})
        feishu_client.send_text_message(
            "chat_id", chat_id,
            f"🔍 开始诊断 {asin} 广告表现...\n⏱️ 预计耗时: {eta}\n"
            "将分析关键词绩效、版位数据及变更历史，请耐心等待。"
        )

        async def _dispatch_job():
            try:
                from src.gateway import APIGateway
                import src.workflows.definitions.ad_diagnosis

                job_id = APIGateway.dispatch_feishu_command(
                    workflow_name="ad_diagnosis",
                    params={"asin": asin},
                    chat_id=chat_id,
                    bot_name=self.bot_name,
                    callback_type="feishu_card",
                )
                logger.info(f"Ad diagnosis routed to Gateway, job_id={job_id}, bot_name={self.bot_name}")
            except Exception as e:
                logger.error(f"Ad diagnosis dispatch failed: {e}")
                feishu_client.send_text_message("chat_id", chat_id, f"❌ 广告诊断任务启动失败: {e}")

        asyncio.run_coroutine_threadsafe(_dispatch_job(), self.loop)


class HelpCommand(BotCommand):
    """响应'常用命令'菜单，列出所有可用指令。"""

    _HELP_TEXT = (
        "📋 **常用命令列表**\n\n"
        "🍪 **更新亚马逊 Cookies**\n"
        "　触发词：`更新亚马逊 Cookies`\n"
        "　作用：启动有头浏览器刷新亚马逊登录凭证\n\n"
        "▶️ **恢复中断任务**\n"
        "　触发词：`恢复任务 <job_id>`\n"
        "　示例：`恢复任务 a1b2c3d4`\n\n"
        "📊 **抓取 BSR 榜单**\n"
        "　触发词：`获取 <类目> BSR`\n"
        "　示例：`获取 Electronics BSR`\n"
        "　支持类目：Electronics / Camera / Software\n\n"
        "🏆 **分析类目垄断度**\n"
        "　触发词：`分析垄断度 <URL>` 或 `分析类目垄断度 <URL>`\n"
        "　示例：`分析垄断度 https://www.amazon.com/...`\n\n"
        "🔎 **产品筛选**\n"
        "　触发词：`产品筛选 <关键词>`\n"
        "　示例：`产品筛选 yoga mat`\n\n"
        "📈 **广告诊断**\n"
        "　触发词：`广告诊断 <ASIN>`\n"
        "　示例：`广告诊断 B0FXFGMD7Z`\n\n"
        "🤖 **智能 Agent（其他问题）**\n"
        "　触发词：任意文本（以上命令未匹配时自动触发）\n"
    )

    def match(self, text: str) -> bool:
        return text.strip() == "常用命令"

    def execute(self, text: str, chat_id: str):
        FeishuClient(bot_name=self.bot_name).send_text_message(
            "chat_id", chat_id, self._HELP_TEXT
        )


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
            HelpCommand(bot_name, loop),
            RefreshCookieCommand(bot_name, loop),
            ResumeJobCommand(bot_name, loop),
            ExtractBSRCommand(bot_name, loop),
            AnalyzeCategoryMonopolyCommand(bot_name, loop),
            ProductScreeningCommand(bot_name, loop),
            AdDiagnosisCommand(bot_name, loop),
            AgentExploreCommand(bot_name, loop),  # fallback — must stay last
        ]
        
    def dispatch(self, text: str, chat_id: str) -> bool:
        for cmd in self.commands:
            if cmd.match(text):
                cmd.execute(text, chat_id); return True
        return False
