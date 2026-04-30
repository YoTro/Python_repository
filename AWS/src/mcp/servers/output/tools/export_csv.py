from __future__ import annotations
import csv
import io
import json
import logging
import os
import uuid
from mcp.types import Tool, TextContent

logger = logging.getLogger("mcp-output-csv")


async def handle_export_csv(name: str, arguments: dict) -> list[TextContent]:
    items    = arguments.get("items", [])
    filename = arguments.get("filename", "export.csv")

    if not items:
        return [TextContent(type="text", text=json.dumps({"success": False, "error": "No items provided"}))]

    try:
        # Build CSV in memory
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(items[0].keys()))
        writer.writeheader()
        writer.writerows(items)
        csv_bytes = buf.getvalue().encode("utf-8-sig")  # BOM for Excel compatibility

        safe_filename = os.path.basename(filename)
        if not safe_filename.endswith(".csv"):
            safe_filename += ".csv"
        key = f"exports/{uuid.uuid4().hex[:8]}_{safe_filename}"

        # Upload via storage backend (R2 / S3 / MinIO / local-HTTP)
        try:
            from src.core.storage import get_storage_backend
            storage = get_storage_backend()
            url = storage.upload(key, csv_bytes, "text/csv; charset=utf-8-sig")
            return [TextContent(type="text", text=json.dumps({"success": True, "url": url}, indent=2))]
        except (ValueError, KeyError):
            # Storage not configured — fall back to local file
            report_dir = os.path.abspath("data/reports")
            os.makedirs(report_dir, exist_ok=True)
            file_path = os.path.join(report_dir, safe_filename)
            with open(file_path, "wb") as f:
                f.write(csv_bytes)
            logger.info(f"Storage not configured; CSV saved locally: {file_path}")
            return [TextContent(type="text", text=json.dumps({"success": True, "file_path": file_path}, indent=2))]

    except Exception as e:
        logger.error(f"Failed to export CSV: {e}")
        return [TextContent(type="text", text=json.dumps({"success": False, "error": str(e)}))]


tools = [
    Tool(
        name="export_csv",
        description=(
            "Exports a list of records to CSV and uploads to the configured storage backend "
            "(R2 / S3 / MinIO / local-HTTP), returning a public URL. "
            "Falls back to a local file path when storage is not configured."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "items":    {"type": "array", "items": {"type": "object"}, "description": "List of flat dicts to export."},
                "filename": {"type": "string", "description": "Output filename, e.g. 'keywords.csv'."},
            },
            "required": ["items"],
        },
    )
]
