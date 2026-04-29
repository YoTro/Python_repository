from __future__ import annotations
import logging
import asyncio
from typing import Dict, Any

from src.jobs.interactions.registry import InteractionRegistry
from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
from src.jobs.manager import get_job_manager

logger = logging.getLogger(__name__)


def _notify_chat(bot_name: str, chat_id: str, message: str) -> None:
    """Best-effort text message to a Feishu chat (fire-and-forget)."""
    if not chat_id:
        return
    try:
        from src.entry.feishu.client import FeishuClient
        FeishuClient(bot_name=bot_name or "amazon_bot").send_text_message(
            "chat_id", chat_id, message
        )
    except Exception as e:
        logger.warning(f"Could not send Feishu notification: {e}")


@InteractionRegistry.register("VERIFY_XIYOU_LOGIN")
async def handle_xiyou_verification(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handler for the 'I have scanned' button click.
    Verifies Xiyou login status and sends a text confirmation to the chat.

    The QR card job is already COMPLETED by the time the user clicks — resume()
    would always return False.  Instead we send a dedicated text message so the
    user gets clear feedback regardless of job state.
    """
    tenant_id = payload.get("tenant_id", "default")
    job_id    = payload.get("job_id")
    chat_id   = payload.get("chat_id")
    bot_name  = payload.get("bot_name", "amazon_bot")

    if not job_id:
        return {"toast": "错误: 缺少 Job ID", "success": False}

    logger.info(f"Interaction: Verifying Xiyou login for tenant={tenant_id}, job={job_id}")

    api = XiyouZhaociAPI(tenant_id=tenant_id)
    # ticket is embedded in the card button value; use it to bypass state file
    ticket = payload.get("ticket")
    result = await asyncio.to_thread(api.check_qr_login_status, ticket)
    status = result.get("status")

    if status == "SUCCESS":
        # Try to resume the job (succeeds when job is still SUSPENDED)
        job_mgr = get_job_manager()
        resumed = job_mgr.resume(job_id)

        if resumed:
            msg = "✅ 西柚登录成功！任务已自动恢复执行，请稍候结果。"
        else:
            msg = "✅ 西柚登录成功！请重新发送您的查询指令以继续。"

        await asyncio.to_thread(_notify_chat, bot_name, chat_id, msg)
        return {"toast": msg, "success": True}

    elif status == "WAITING":
        return {
            "toast": "⏳ 尚未扫码或确认，请在手机端点击「允许登录」后重试。",
            "success": False,
        }

    elif status == "EXPIRED":
        msg = "❌ 二维码已过期，请重新发起查询任务以获取新二维码。"
        await asyncio.to_thread(_notify_chat, bot_name, chat_id, msg)
        return {"toast": msg, "success": False}

    else:
        msg = f"❌ 验证失败: {result.get('msg', '未知错误')}"
        await asyncio.to_thread(_notify_chat, bot_name, chat_id, msg)
        return {"toast": msg, "success": False}
