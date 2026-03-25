from __future__ import annotations
import logging
import asyncio
from typing import Dict, Any

from src.jobs.interactions.registry import InteractionRegistry
from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
from src.jobs.manager import get_job_manager

logger = logging.getLogger(__name__)

@InteractionRegistry.register("VERIFY_XIYOU_LOGIN")
async def handle_xiyou_verification(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handler for the 'I have scanned' button click.
    Verifies login status and resumes the job if successful.
    """
    tenant_id = payload.get("tenant_id", "default")
    job_id = payload.get("job_id")
    
    if not job_id:
        return {"toast": "错误: 缺少 Job ID", "success": False}

    logger.info(f"Interaction: Verifying Xiyou login for tenant={tenant_id}, job={job_id}")
    
    api = XiyouZhaociAPI(tenant_id=tenant_id)
    # Perform a single status check
    result = await asyncio.to_thread(api.check_qr_login_status)
    status = result.get("status")
    
    if status == "SUCCESS":
        # Resume the hung job
        job_mgr = get_job_manager()
        resumed = job_mgr.resume(job_id)
        
        if resumed:
            return {
                "toast": "✅ 登录成功！任务已自动恢复执行。",
                "success": True,
                "card_update": "status_success"
            }
        else:
            return {
                "toast": "✅ 登录成功，但无法自动恢复任务，请尝试手动重试。",
                "success": True
            }
            
    elif status == "WAITING":
        return {
            "toast": "⏳ 尚未扫码或确认，请在手机端点击'允许登录'后重试。",
            "success": False
        }
        
    elif status == "EXPIRED":
        return {
            "toast": "❌ 二维码已过期，请重新发起查询任务。",
            "success": False,
            "card_update": "status_expired"
        }
        
    else:
        msg = result.get("msg", "未知错误")
        return {"toast": f"❌ 验证失败: {msg}", "success": False}
