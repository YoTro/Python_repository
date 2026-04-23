from __future__ import annotations
import os
import logging
import asyncio
from typing import Optional, TypeVar, Any, List, Dict
from pydantic import BaseModel
from google import genai
from google.genai import types
from .base import BaseLLMProvider
from src.intelligence.dto import LLMResponse, BatchRequest, BatchJobHandle

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

class GeminiProvider(BaseLLMProvider):
    """
    Ultra-robust Gemini Provider with Auto-Model-Discovery and Cost Calculation.
    """

    # Context windows per model family (prefix-matched against self.model_name).
    # Gemini 1.5 Pro has a 2M window; all other current models are 1M.
    _MODEL_CONTEXT_WINDOWS = {
        "models/gemini-2.5-pro":    1_048_576,
        "models/gemini-2.5-flash":  1_048_576,
        "models/gemini-2.0-flash":  1_048_576,
        "models/gemini-2.0-pro":    1_048_576,
        "models/gemini-1.5-pro":    2_097_152,
        "models/gemini-1.5-flash":  1_048_576,
        "models/gemini-1.0-pro":       32_760,
    }

    def __init__(self,
                 api_key: Optional[str] = None,
                 model_name: Optional[str] = None):

        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY missing.")

        self.client = genai.Client(api_key=self.api_key)
        
        discovered_model = self._discover_best_model(model_name)
        super().__init__("gemini", discovered_model)
        
        logger.info(f"GeminiProvider initialized with discovered model: {self.model_name}")

    def _discover_best_model(self, preferred: Optional[str]) -> str:
        """Query the API to find the highest-tier available model."""
        try:
            # Try newer attribute first, then fallback to older
            all_models = self.client.models.list()
            available = []
            for m in all_models:
                if hasattr(m, 'supported_generation_methods') and 'generateContent' in m.supported_generation_methods:
                    available.append(m.name)
                elif hasattr(m, 'supported_actions') and "generateContent" in m.supported_actions:
                    available.append(m.name)

            priorities = [
                "models/gemini-2.5-flash",
                "models/gemini-1.5-flash",
                "models/gemini-1.5-pro",
            ]

            if preferred and preferred in available:
                return preferred

            for p in priorities:
                if p in available:
                    return p

            return available[0] if available else "models/gemini-1.5-flash"
        except Exception as e:
            logger.error(f"Failed to list models: {e}. Falling back to default.")
            return "models/gemini-1.5-flash"

    async def count_tokens(self, prompt: str, system_message: Optional[str] = None) -> int:
        try:
            full_text = f"{system_message}\n\n{prompt}" if system_message else prompt
            response = await asyncio.to_thread(
                self.client.models.count_tokens,
                model=self.model_name,
                contents=full_text
            )
            return response.total_tokens
        except Exception:
            return len(prompt) // 4

    async def generate_text(self, prompt: str, system_message: Optional[str] = None, **kwargs) -> LLMResponse:
        await self._check_context_limit(prompt, system_message)
        try:
            # Filter out internal metadata from kwargs
            filtered_kwargs = self._filter_kwargs(kwargs)
            
            # Extract and handle temperature (default to 0.2)
            temp = filtered_kwargs.pop("temperature", 0.2)

            config = types.GenerateContentConfig(
                system_instruction=system_message,
                temperature=temp,
            ) if system_message else types.GenerateContentConfig(
                temperature=temp,
            )

            response = await asyncio.to_thread(
                self.client.models.generate_content,
                model=self.model_name,
                contents=prompt,
                config=config,
                **filtered_kwargs
            )
            
            usage = getattr(response, "usage_metadata", None)
            input_tokens = usage.prompt_token_count if usage else await self.count_tokens(prompt, system_message)
            output_tokens = usage.candidates_token_count if usage else 0
            
            # Extract advanced usage stats for precise billing
            thought_tokens = getattr(usage, "thought_token_count", 0) or 0
            cached_tokens = getattr(usage, "cached_content_token_count", 0) or 0
            
            return self.create_response(
                text=response.text,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                thought_tokens=thought_tokens,
                cached_tokens=cached_tokens
            )
        except Exception as e:
            logger.error(f"Gemini text generation failed: {e}")
            raise

    @staticmethod
    def _clean_schema(schema: dict) -> dict:
        """Remove properties unsupported by the Gemini API (e.g. additionalProperties)."""
        UNSUPPORTED = {"additionalProperties", "$schema", "title"}
        result = {}
        for k, v in schema.items():
            if k in UNSUPPORTED:
                continue
            if isinstance(v, dict):
                result[k] = GeminiProvider._clean_schema(v)
            elif isinstance(v, list):
                result[k] = [
                    GeminiProvider._clean_schema(i) if isinstance(i, dict) else i
                    for i in v
                ]
            else:
                result[k] = v
        return result

    # ── Batch API ─────────────────────────────────────────────────────────────

    def supports_batch(self) -> bool:
        return True

    async def generate_batch(self, requests: List[BatchRequest]) -> BatchJobHandle:
        """Submit an inline batch job. Returns immediately with a handle.

        SDK v1.67+: src accepts a list[InlinedRequest] directly; each InlinedRequest
        carries custom_id in metadata so we can map responses back by key.
        """
        self._check_batch_context_limit_sync(requests)
        try:
            inline_requests = []
            for req in requests:
                config = None
                if req.schema or req.system_message:
                    schema_dict = self._clean_schema(req.schema.model_json_schema()) if req.schema else None
                    config = types.GenerateContentConfig(
                        system_instruction=req.system_message,
                        response_mime_type="application/json" if schema_dict else None,
                        response_schema=schema_dict,
                    )
                inline_requests.append(
                    types.InlinedRequest(
                        model=self.model_name,
                        contents=req.prompt,
                        config=config,
                        metadata={"custom_id": req.custom_id},
                    )
                )

            batch_job = await asyncio.to_thread(
                self.client.batches.create,
                model=self.model_name,
                src=inline_requests,
            )
            logger.info(f"Gemini batch submitted: {batch_job.name}, {len(requests)} requests")
            return BatchJobHandle(
                job_id=batch_job.name,
                provider="gemini",
                status="pending",
            )
        except Exception as e:
            logger.error(f"Gemini batch submission failed: {e}")
            raise

    async def poll_batch(self, handle: BatchJobHandle) -> Optional[Dict[str, LLMResponse]]:
        """Check batch status. Returns None while pending; dict on completion.

        SDK v1.67+: completed results are in job.dest.inlined_responses (same
        order as input requests). custom_id is recovered from resp.metadata.
        """
        _TERMINAL = {"JOB_STATE_SUCCEEDED", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED"}
        try:
            job = await asyncio.to_thread(self.client.batches.get, name=handle.job_id)
            # job.state is a JobState enum; use .name to get "JOB_STATE_SUCCEEDED" etc.
            raw_state = getattr(job, "state", None)
            state = getattr(raw_state, "name", str(raw_state)).upper()

            if state not in _TERMINAL:
                logger.debug(f"Gemini batch {handle.job_id} state={state} (raw={job.state})")
                return None

            if state != "JOB_STATE_SUCCEEDED":
                raise RuntimeError(f"Gemini batch {handle.job_id} ended with state={state}")

            inlined_responses = (
                (job.dest.inlined_responses or []) if job.dest else []
            )
            results: Dict[str, LLMResponse] = {}
            for resp in inlined_responses:
                custom_id = (resp.metadata or {}).get("custom_id")
                if not custom_id:
                    logger.warning(f"Gemini batch response missing custom_id metadata, skipping")
                    continue
                if getattr(resp, "error", None):
                    logger.warning(f"Gemini batch item error custom_id={custom_id}: {resp.error}")
                    continue
                gc_response = resp.response
                usage = getattr(gc_response, "usage_metadata", None)
                input_tokens = usage.prompt_token_count if usage else 0
                output_tokens = usage.candidates_token_count if usage else 0
                thought_tokens = getattr(usage, "thought_token_count", 0) or 0
                cached_tokens = getattr(usage, "cached_content_token_count", 0) or 0
                results[custom_id] = self.create_response(
                    text=gc_response.text,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    thought_tokens=thought_tokens,
                    cached_tokens=cached_tokens,
                    is_batch=True,
                )
            logger.info(f"Gemini batch {handle.job_id} complete: {len(results)} results")
            return results
        except Exception as e:
            logger.error(f"Gemini batch poll failed: {e}")
            raise

    # ─────────────────────────────────────────────────────────────────────────

    async def generate_structured(self, prompt: str, schema: Any, system_message: Optional[str] = None, **kwargs) -> LLMResponse:
        await self._check_context_limit(prompt, system_message)
        try:
            raw_schema = schema.model_json_schema()
            clean = self._clean_schema(raw_schema)

            # Filter out internal metadata from kwargs
            filtered_kwargs = self._filter_kwargs(kwargs)

            response = await asyncio.to_thread(
                self.client.models.generate_content,
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=system_message,
                    response_mime_type="application/json",
                    response_schema=clean,
                ),
                **filtered_kwargs
            )
            
            # Since we're asking for a schema, the text should be valid JSON
            text_response = response.text
            
            usage = getattr(response, "usage_metadata", None)
            input_tokens = usage.prompt_token_count if usage else 0
            output_tokens = usage.candidates_token_count if usage else 0
            
            # Extract advanced usage stats for precise billing
            thought_tokens = getattr(usage, "thought_token_count", 0) or 0
            cached_tokens = getattr(usage, "cached_content_token_count", 0) or 0
            
            return self.create_response(
                text=text_response,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                thought_tokens=thought_tokens,
                cached_tokens=cached_tokens
            )
        except Exception as e:
            logger.error(f"Structured generation failed on {self.model_name}: {e}")
            raise
