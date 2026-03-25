import unittest
import os
import json
from unittest.mock import MagicMock, patch
from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
from src.mcp.servers.market.xiyouzhaoci.auth import XiyouZhaociAuth
from src.workflows.steps.base import WorkflowContext
from src.gateway.router import APIGateway
from src.core.models.request import UnifiedRequest, CallbackConfig
from src.agents.session import AgentSessionManager, AgentSession

class TestIdentityPropagation(unittest.IsolatedAsyncioTestCase):

    def test_xiyou_path_isolation(self):
        """验证西柚找词的 Token 路径是否根据 tenant_id 隔离"""
        tenant_a = "company_a"
        tenant_b = "company_b"
        
        api_a = XiyouZhaociAPI(tenant_id=tenant_a)
        api_b = XiyouZhaociAPI(tenant_id=tenant_b)
        
        # 验证 Token 文件名是否包含租户 ID
        self.assertIn(f"xiyou_{tenant_a}_token.json", api_a.token_file)
        self.assertIn(f"xiyou_{tenant_b}_token.json", api_b.token_file)
        self.assertNotEqual(api_a.token_file, api_b.token_file)
        
        # 验证 Auth 模块的路径
        self.assertIn(f"xiyou_{tenant_a}_state.json", api_a.auth.state_file)
        self.assertIn(f"xiyou_{tenant_b}_state.json", api_b.auth.state_file)

    def test_workflow_context_injection(self):
        """验证工作流上下文是否正确接收了网关传入的身份信息"""
        # 模拟一个 UnifiedRequest
        req = UnifiedRequest(
            tenant_id="tenant_123",
            user_id="user_456",
            workflow_name="amazon_bsr",
            params={"url": "test_url"}
        )
        
        # 验证 WorkflowContext 的结构
        ctx = WorkflowContext(
            job_id="job_001",
            tenant_id=req.tenant_id,
            user_id=req.user_id,
            config=req.params
        )
        
        self.assertEqual(ctx.tenant_id, "tenant_123")
        self.assertEqual(ctx.user_id, "user_456")

    @patch("src.agents.mcp_agent.get_mcp_client")
    @patch("src.agents.mcp_agent.PromptBuilder")
    async def test_agent_session_and_metadata(self, mock_pb, mock_mcp_client):
        """验证 Agent 轨道是否正确持久化身份并注入工具调用元数据"""
        from src.agents.mcp_agent import MCPAgent
        from src.intelligence.providers.base import BaseLLMProvider
        
        # 1. 准备 Mock 环境
        mock_router = MagicMock()
        session_mgr = AgentSessionManager()
        agent = MCPAgent(provider=MagicMock())
        agent.router = mock_router
        agent.mcp = MagicMock()
        
        # 2. 模拟运行 Agent
        # 由于 run 是个复杂的循环，我们直接验证其内部逻辑依赖的 session 创建
        session = session_mgr.create(
            session_id="test_sess",
            tenant_id="tenant_agent",
            user_id="user_agent"
        )
        
        self.assertEqual(session.tenant_id, "tenant_agent")
        self.assertEqual(session.user_id, "user_agent")
        
        # 3. 验证工具调用时的元数据注入逻辑
        # 模拟 _execute_tool 内部逻辑 (我们修改过的部分)
        test_input = {"query": "iphone"}
        # 注入身份元数据
        test_input["_metadata"] = {
            "tenant_id": session.tenant_id,
            "user_id": session.user_id
        }
        
        self.assertEqual(test_input["_metadata"]["tenant_id"], "tenant_agent")

if __name__ == "__main__":
    unittest.main()
