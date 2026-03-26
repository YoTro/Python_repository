from __future__ import annotations
import json
import logging
from typing import Any, Optional
from src.agents.base_agent import BaseAgent
from src.intelligence.providers.base import BaseLLMProvider
from src.intelligence.router import TaskCategory
from src.mcp.client import get_mcp_client
from src.agents.session import AgentSessionManager, AgentSession
from src.agents.prompts.prompt_builder import PromptBuilder
from src.registry.tools import tool_registry
from src.core.utils.context import ContextPropagator
from src.core.errors.exceptions import JobSuspendedError

logger = logging.getLogger(__name__)

_MAX_DUPLICATE_CALLS = 2   # abort tool loop after N identical consecutive calls
_DEFAULT_TOKEN_BUDGET = 1000000  # cumulative *cloud* token threshold before switching to batch
_LOCAL_PROVIDERS = {"local", "llama_cpp", "llama"}  # provider names that run locally (free)


class MCPAgent(BaseAgent):
    """
    Agent that uses a ReAct-style loop to dynamically interact with MCP Tools.
    Maintains memory and context across turns using AgentSession.

    Token-budget strategy:
      - Steps are for progress display only, NOT a hard failure limit.
      - Cumulative cloud token usage is tracked (local model tokens are free).
      - When cloud usage exceeds the budget, the agent forces a final summary
        and notifies the user that remaining work switches to batch API.
    """
    def __init__(
        self,
        provider: BaseLLMProvider,
        session_mgr: Optional[AgentSessionManager] = None,
        token_budget: int = _DEFAULT_TOKEN_BUDGET,
    ):
        super().__init__(provider)
        self.mcp = get_mcp_client()
        self.session_mgr = session_mgr or AgentSessionManager()
        self._prompt_builder = PromptBuilder()
        self.token_budget = token_budget

    # ── helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _accum_tokens(session: AgentSession, response_obj) -> None:
        """Accumulate token usage and monetary cost."""
        if not response_obj:
            return
            
        if response_obj.token_usage:
            session.token_usage += response_obj.token_usage
            if response_obj.provider_name not in _LOCAL_PROVIDERS:
                session.cloud_token_usage += response_obj.token_usage
        
        if hasattr(response_obj, "cost") and response_obj.cost:
            session.total_cost += response_obj.cost
            if hasattr(response_obj, "currency"):
                session.currency = response_obj.currency

    @staticmethod
    def _parse_tool_call(response: str) -> tuple[Optional[str], Optional[dict]]:
        """Extract (action, action_input) from an LLM response. Returns (None, None) on failure."""
        try:
            # Use unified logic from OutputParser
            from src.intelligence.parsers.markdown_cleaner import OutputParser
            call = OutputParser.parse_dirty_json(response)
            
            if not call:
                return None, None
                
            return call.get("action"), call.get("action_input", {})
        except Exception as e:
            logger.error(f"Error parsing tool call: {e}")
            return None, None

    async def _force_final_answer(self, session: AgentSession, system_message: str, reason: str) -> str:
        """
        Ask the LLM one last time to produce a Final Answer using
        all the data it has already collected.
        """
        session.add_message(
            role="tool", name="system",
            content=(
                f"SYSTEM: {reason} "
                "You MUST now produce your Final Answer using ALL the data you have collected so far. "
                "Do NOT call any more tools. Start your reply with 'Final Answer:'."
            ),
        )
        conversation = session.format_history_as_text()
        response_obj = await self.router.route_and_execute(
            conversation, system_message=system_message, category=TaskCategory.DEEP_REASONING
        )
        response = response_obj.text if response_obj else ""
        self._accum_tokens(session, response_obj)
        session.add_message(role="assistant", content=response)

        if "Final Answer:" in response:
            return response.split("Final Answer:")[-1].strip()
        # LLM still didn't comply — return whatever it said
        return response.strip()

    # ── main loop ─────────────────────────────────────────────────────────

    async def run(
        self, 
        query: str, 
        session_id: str = "default_session", 
        tenant_id: str = "default",
        user_id: str = "default",
        callback=None,
        context: Optional[dict] = None
    ) -> str:
        # 1. Load or Create Session
        session = self.session_mgr.load(session_id)
        if not session:
            session = self.session_mgr.create(
                session_id=session_id,
                tenant_id=tenant_id,
                user_id=user_id
            )
        
        # Merge external context (chat_id, etc.) if provided
        if context:
            session.context.update(context)

        # Add the new query only if this is a fresh session
        if query and not session.history:
            session.add_message(role="user", content=query)

        system_message = self._prompt_builder.build(
            tool_registry, max_steps=session.max_steps, token_budget=self.token_budget
        )

        # Duplicate tool call tracking
        last_tool_call = None
        duplicate_count = 0
        budget_exceeded = False
        step_extensions = 0          # number of grace-period extensions granted
        _MAX_EXTENSIONS = 2          # hard cap: at most 2 extensions (10 extra steps total)

        # 2. Execution Loop
        token = ContextPropagator.set_all(session.context)
        try:
            while True:
                session.current_step += 1
                logger.info(
                    f"MCPAgent [{session.session_id}] Step {session.current_step}/{session.max_steps} "
                    f"(tenant: {session.tenant_id}, cloud tokens: {session.cloud_token_usage}/{self.token_budget}, "
                    f"cost: {session.total_cost:.4f} {session.currency})"
                )

                # ── Step limit and Token budget management ────────────────────
                
                # Scenario A: Steps exceeded but Token budget is healthy (>20% left)
                if session.current_step > session.max_steps:
                    if session.cloud_token_usage < (self.token_budget * 0.8) and step_extensions < _MAX_EXTENSIONS:
                        # Grant a grace period of 5 steps
                        step_extensions += 1
                        old_max = session.max_steps
                        session.max_steps += 5
                        logger.info(f"Step limit {old_max} reached, but budget is healthy. Extending to {session.max_steps} (extension {step_extensions}/{_MAX_EXTENSIONS}).")
                        session.add_message(
                            role="tool", name="system",
                            content=(
                                f"SYSTEM: You have reached the initial step limit ({old_max}). "
                                "Because you still have sufficient token budget, I have granted you 5 more steps. "
                                f"This is grace extension {step_extensions}/{_MAX_EXTENSIONS}. "
                                "Please focus on CONVERGING your research and provide a Final Answer soon."
                            )
                        )
                    else:
                        # Token budget is tight or steps really exhausted — force closure
                        logger.warning(f"Step limit {session.max_steps} reached and budget is low. Forcing final answer.")
                        answer = await self._force_final_answer(
                            session, system_message,
                            reason=f"Step limit ({session.max_steps}) reached. Please summarize your findings."
                        )
                        session.status = "completed"
                        self.session_mgr.save(session)
                        return answer

                # Persist progress
                self.session_mgr.save(session)

                # Progress callback
                if callback:
                    try:
                        await callback.on_progress(
                            step_index=session.current_step,
                            total_steps=session.max_steps,
                            step_name=f"Agent Reasoning (Step {session.current_step})",
                            message=(
                                f"Cloud tokens: {session.cloud_token_usage}/{self.token_budget}, "
                                f"Cost: {session.total_cost:.4f} {session.currency}. "
                                "Consulting LLM..."
                            ),
                        )
                    except Exception as e:
                        logger.warning(f"Agent callback on_progress failed: {e}")

                # Scenario B: Hard Token budget exceeded
                if not budget_exceeded and session.cloud_token_usage >= self.token_budget:
                    budget_exceeded = True
                    logger.warning(
                        f"Cloud token budget exceeded ({session.cloud_token_usage}/{self.token_budget}). "
                        "Forcing final summary."
                    )
                    if callback:
                        try:
                            await callback.on_progress(
                                step_index=session.current_step,
                                total_steps=session.max_steps,
                                step_name="Token Budget Exceeded",
                                message=(
                                    f"Cloud token usage ({session.cloud_token_usage}) exceeded "
                                    f"budget ({self.token_budget}). Switching to batch mode for "
                                    "remaining analysis. Generating summary of collected data..."
                                ),
                            )
                        except Exception:
                            pass

                    answer = await self._force_final_answer(
                        session, system_message,
                        reason=(
                            f"Cloud token budget exhausted ({session.cloud_token_usage} cloud tokens used). "
                            "Remaining analysis will be processed via batch API."
                        ),
                    )
                    session.status = "completed"
                    self.session_mgr.save(session)
                    return answer

                # ── LLM call ──────────────────────────────────────────────────
                conversation = session.format_history_as_text()

                response_obj = await self.router.route_and_execute(
                    conversation, system_message=system_message, category=TaskCategory.DEEP_REASONING
                )
                response = response_obj.text if response_obj else ""
                self._accum_tokens(session, response_obj)
                logger.debug(
                    f"LLM Response via {response_obj.provider_name if response_obj else 'N/A'}: "
                    f"{len(response)} chars, {response_obj.token_usage if response_obj else 0} tokens"
                )

                # ── Final Answer detection ────────────────────────────────────
                if "Final Answer:" in response:
                    final_answer = response.split("Final Answer:")[-1].strip()
                    session.add_message(role="assistant", content=response)
                    session.status = "completed"
                    self.session_mgr.save(session)
                    return final_answer

                # ── Parse tool call ───────────────────────────────────────────
                action, action_input = self._parse_tool_call(response)
                session.add_message(role="assistant", content=response)

                if action:
                    # Duplicate tool call detection
                    current_call = (action, json.dumps(action_input, sort_keys=True))
                    if current_call == last_tool_call:
                        duplicate_count += 1
                        if duplicate_count >= _MAX_DUPLICATE_CALLS:
                            logger.warning(
                                f"Duplicate tool call detected {duplicate_count} times: "
                                f"{action}({action_input}). Injecting hint."
                            )
                            session.add_message(
                                role="tool", name="system",
                                content=(
                                    f"ERROR: You have called {action} with identical arguments "
                                    f"{duplicate_count} times. The result will be the same. "
                                    f"Either change the arguments (e.g. increment 'page') or "
                                    f"produce your Final Answer from the data you already have."
                                ),
                            )
                            duplicate_count = 0
                            last_tool_call = None
                            continue
                    else:
                        duplicate_count = 1
                        last_tool_call = current_call

                    logger.info(f"LLM called tool: {action} with args {action_input}")
                    try:
                        # Inject identity metadata into the tool call
                        action_input["_metadata"] = {
                            "tenant_id": session.tenant_id,
                            "user_id": session.user_id,
                            "job_id": session.session_id,
                            "chat_id": session.context.get("feishu_chat_id")
                        }
                        result = await self.mcp.call_tool_json(action, action_input)
                        
                        # --- INTERACTION SIGNAL INTERCEPTION ---
                        # If the tool returned a structural interaction signal, intercept it.
                        if isinstance(result, dict) and result.get("_type") == "INTERACTION_REQUIRED":
                            logger.info(f"Tool {action} returned an interaction signal. Pausing agent loop.")
                            
                            # Log the interaction request to the session history so the agent knows what happened
                            session.add_message(
                                role="tool", 
                                content=f"Sent interaction request to user: {result.get('fallback_text')}", 
                                name=action
                            )
                            
                            # Mark session as suspended waiting for human input
                            session.status = "suspended_for_human"
                            self.session_mgr.save(session)
                            
                            # If we have a callback (like Feishu), forward the RAW signal directly to it
                            if callback:
                                try:
                                    signal_json = json.dumps(result, ensure_ascii=False)
                                    # We use on_progress to push the interactive card immediately
                                    await callback._send_progress(signal_json)
                                except Exception as e:
                                    logger.error(f"Failed to forward interaction signal to callback: {e}")
                            
                            # Raise exception to signal suspension to the JobManager
                            raise JobSuspendedError(
                                message=result.get("fallback_text", "Interaction required."),
                                signal=result
                            )

                        observation = json.dumps(result, ensure_ascii=False)
                    except Exception as e:
                        observation = f"Error calling tool: {e}"

                    session.add_message(role="tool", content=observation, name=action)
                else:
                    # No tool call and no Final Answer — assume conversational reply
                    session.status = "completed"
                    self.session_mgr.save(session)
                    return response.strip()
        finally:
            ContextPropagator.reset(token)
