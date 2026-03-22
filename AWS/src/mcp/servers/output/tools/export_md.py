from __future__ import annotations
import json
import logging
import os
from mcp.types import Tool, TextContent

logger = logging.getLogger("mcp-output-md")

async def handle_export_md(name: str, arguments: dict) -> list[TextContent]:
    content = arguments.get("content", "")
    filename = arguments.get("filename", "report.md")
    
    try:
        # Save to data/reports in the project root
        report_dir = os.path.abspath("data/reports")
        os.makedirs(report_dir, exist_ok=True)
        
        # Ensure filename is safe and has .md extension
        safe_filename = os.path.basename(filename)
        if not safe_filename.endswith(".md"):
            safe_filename += ".md"
            
        file_path = os.path.join(report_dir, safe_filename)
        
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
            
        logger.info(f"Markdown report exported to: {file_path}")
        return [TextContent(type="text", text=json.dumps({"success": True, "file_path": file_path}, indent=2))]
    except Exception as e:
        logger.error(f"Failed to export markdown: {e}")
        return [TextContent(type="text", text=json.dumps({"success": False, "error": str(e)}))]

tools = [
    Tool(
        name="export_md",
        description="Saves markdown content to a local file and returns the absolute file path. Use this when the user requests a file attachment or when the content exceeds the 30KB card limit.",
        inputSchema={
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "The full markdown content to save."},
                "filename": {"type": "string", "description": "The desired filename (e.g., 'tiktok_analysis.md')."}
            },
            "required": ["content", "filename"]
        }
    )
]
