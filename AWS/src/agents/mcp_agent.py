from __future__ import annotations
import json
import logging
from typing import Any, Optional
from src.agents.base_agent import BaseAgent
from src.intelligence.providers.base import BaseLLMProvider
from src.mcp.client import get_mcp_client
from src.agents.session import AgentSessionManager, AgentSession

logger = logging.getLogger(__name__)

class MCPAgent(BaseAgent):
    """
    Agent that uses a ReAct-style loop to dynamically interact with MCP Tools.
    Maintains memory and context across turns using AgentSession.
    """
    def __init__(self, provider: BaseLLMProvider, session_mgr: Optional[AgentSessionManager] = None):
        super().__init__(provider)
        self.mcp = get_mcp_client()
        self.session_mgr = session_mgr or AgentSessionManager()

    async def run(self, query: str, session_id: str = "default_session", callback=None) -> str:
        # 1. Load or Create Session
        session = self.session_mgr.load(session_id)
        if not session:
            session = self.session_mgr.create(session_id=session_id)
            
        # Add the new query if it's not a resumed session without a query
        if query:
            session.add_message(role="user", content=query)
            
        tools = await self.mcp.list_tools()
        
        tool_descriptions = []
        for t in tools:
            tool_descriptions.append(
                f"Tool Name: {t.name}\nDescription: {t.description}\nInput Schema: {json.dumps(t.inputSchema)}"
            )
        
        tools_str = "\n\n".join(tool_descriptions)

        system_message = f"""You are an AWS (Amazon Web Scraper) MCP Agent capable of deep market research.
You have access to the following tools:

{tools_str}

To answer the user's query, you can use these tools to gather information.
If you need to use a tool, you MUST reply with a JSON block in this exact format:
```json
{{
    "action": "tool_name",
    "action_input": {{"arg1": "value"}}
}}
```

After you output the JSON block, STOP writing. The system will provide you with the Observation.
Once you have gathered enough information to answer the user, reply with your final answer prefixed with "Final Answer: ".
"""
        
        # 2. Execution Loop
        while session.current_step < session.max_steps:
            session.current_step += 1
            logger.info(f"MCPAgent [{session.session_id}] Iteration {session.current_step}/{session.max_steps}")
            
            # Persist progress
            self.session_mgr.save(session)
            
            if callback:
                try:
                    await callback.on_progress(
                        step_index=session.current_step,
                        total_steps=session.max_steps,
                        step_name=f"Agent Reasoning (Step {session.current_step})",
                        message="Consulting LLM or fetching tool data..."
                    )
                except Exception as e:
                    logger.warning(f"Agent callback on_progress failed: {e}")
            
            # Format history for the LLM
            conversation = session.format_history_as_text()
            
            # CORE CHANGE: Use router instead of direct provider call
            response_obj = await self.router.route_and_execute(conversation, system_message=system_message)
            response = response_obj.text if response_obj else ""
            logger.debug(f"LLM Response via {response_obj.provider_name if response_obj else 'N/A'}: {response}")
            
            if "Final Answer:" in response:
                final_answer = response.split("Final Answer:")[-1].strip()
                session.add_message(role="assistant", content=response)
                session.status = "completed"
                self.session_mgr.save(session)
                return final_answer
            
            # Try to parse tool call
            action = None
            action_input = None
            try:
                if "```json" in response:
                    json_str = response.split("```json")[1].split("```")[0].strip()
                    call = json.loads(json_str)
                    action = call.get("action")
                    action_input = call.get("action_input", {})
                elif "{" in response and "}" in response:
                    # Fallback for LLMs that forget backticks
                    json_str = response[response.find("{"):response.rfind("}")+1]
                    call = json.loads(json_str)
                    action = call.get("action")
                    action_input = call.get("action_input", {})
            except Exception as e:
                logger.warning(f"Failed to parse tool call from LLM response: {e}")
            
            # Record Assistant's thought/action
            session.add_message(role="assistant", content=response)
            
            if action:
                logger.info(f"LLM called tool: {action} with args {action_input}")
                try:
                    result = await self.mcp.call_tool_json(action, action_input)
                    observation = json.dumps(result, ensure_ascii=False)
                except Exception as e:
                    observation = f"Error calling tool: {e}"
                
                # Record Tool's observation
                session.add_message(role="tool", content=observation, name=action)
            else:
                # If it didn't call a tool and didn't say Final Answer, assume it's conversing
                session.status = "completed"
                self.session_mgr.save(session)
                return response.strip()

        session.status = "failed"
        self.session_mgr.save(session)
        return "Agent reached maximum iterations without completing the task."
