from __future__ import annotations
import json
import logging
import os
from mcp.types import Tool, TextContent

logger = logging.getLogger("mcp-output-html")

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", sans-serif;
    max-width: 960px; margin: 40px auto; padding: 0 24px;
    color: #1a1a1a; line-height: 1.7; background: #fff;
  }}
  h1 {{ border-bottom: 2px solid #e0e0e0; padding-bottom: 10px; margin-top: 32px; }}
  h2 {{ border-bottom: 1px solid #f0f0f0; padding-bottom: 6px; color: #222; margin-top: 28px; }}
  h3 {{ color: #333; margin-top: 20px; }}
  table {{
    border-collapse: collapse; width: 100%; margin: 16px 0; font-size: 14px;
    box-shadow: 0 1px 3px rgba(0,0,0,.06);
  }}
  th {{
    background: #f7f7f7; border: 1px solid #e0e0e0;
    padding: 9px 14px; text-align: left; font-weight: 600;
  }}
  td {{ border: 1px solid #e0e0e0; padding: 8px 14px; }}
  tr:nth-child(even) {{ background: #fafafa; }}
  code {{
    background: #f4f4f4; padding: 2px 6px;
    border-radius: 4px; font-size: 13px; font-family: "SFMono-Regular", Consolas, monospace;
  }}
  pre {{
    background: #f6f8fa; border: 1px solid #e8e8e8;
    padding: 16px; border-radius: 6px; overflow-x: auto; line-height: 1.5;
  }}
  pre code {{ background: none; padding: 0; font-size: 13px; }}
  blockquote {{
    border-left: 4px solid #d0d7de; margin: 0 0 16px; padding: 4px 16px;
    color: #57606a; background: #f6f8fa;
  }}
  a {{ color: #0969da; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  hr {{ border: none; border-top: 1px solid #e0e0e0; margin: 24px 0; }}
  .report-meta {{
    color: #666; font-size: 13px; margin-bottom: 24px;
    padding: 8px 14px; background: #f6f8fa; border-radius: 4px;
  }}
</style>
</head>
<body>
{body}
</body>
</html>
"""


def _md_to_html(md: str) -> str:
    """Convert markdown to HTML. Uses `markdown` package; falls back to <pre> wrap."""
    try:
        import markdown as _md
        return _md.markdown(
            md,
            extensions=["tables", "fenced_code", "nl2br", "toc"],
        )
    except ImportError:
        import html as _html
        return f"<pre style='white-space:pre-wrap'>{_html.escape(md)}</pre>"


def _upload_images(content: str, image_paths: list[str]) -> tuple[str, list[str]]:
    """
    Upload local image files to the configured storage backend and replace
    any matching local path references in content with public HTTPS URLs.

    Returns (updated_content, list_of_public_urls).
    Gracefully skips upload if STORAGE_BACKEND is not configured.
    """
    if not image_paths:
        return content, []

    try:
        from src.core.storage import get_storage_backend
        storage = get_storage_backend()
    except (ValueError, KeyError) as e:
        logger.warning(f"Storage backend not configured, images skipped: {e}")
        return content, []

    import mimetypes
    import uuid

    public_urls: list[str] = []
    for local_path in image_paths:
        if not os.path.exists(local_path):
            logger.warning(f"Image not found, skipping: {local_path}")
            continue
        mime = mimetypes.guess_type(local_path)[0] or "image/png"
        ext  = os.path.splitext(local_path)[1] or ".png"
        key  = f"reports/{uuid.uuid4().hex}{ext}"
        try:
            url = storage.upload_file(key, local_path, mime)
            public_urls.append(url)
            content = content.replace(local_path, url)
        except Exception as e:
            logger.error(f"Failed to upload {local_path}: {e}")

    return content, public_urls


async def handle_export_html(name: str, arguments: dict) -> list[TextContent]:
    content      = arguments.get("content", "")
    filename     = arguments.get("filename", "report.html")
    title        = arguments.get("title", "Report")
    is_markdown  = arguments.get("is_markdown", True)
    image_paths  = arguments.get("images", [])   # list of local file paths to upload

    try:
        # Upload images and rewrite references before markdown conversion
        if image_paths:
            content, uploaded = _upload_images(content, image_paths)
            logger.info(f"Uploaded {len(uploaded)} image(s) to storage")

        report_dir = os.path.abspath("data/reports")
        os.makedirs(report_dir, exist_ok=True)

        safe_filename = os.path.basename(filename)
        if not safe_filename.endswith(".html"):
            safe_filename += ".html"

        body         = _md_to_html(content) if is_markdown else content
        html_content = _HTML_TEMPLATE.format(title=title, body=body)

        file_path = os.path.join(report_dir, safe_filename)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        logger.info(f"HTML report exported to: {file_path}")
        return [TextContent(
            type="text",
            text=json.dumps({"success": True, "file_path": file_path}, indent=2),
        )]
    except Exception as e:
        logger.error(f"Failed to export HTML: {e}")
        return [TextContent(
            type="text",
            text=json.dumps({"success": False, "error": str(e)}),
        )]


tools = [
    Tool(
        name="export_html",
        description=(
            "Converts content to a styled HTML file and saves it to data/reports/. "
            "Accepts markdown (auto-converted to HTML with table, fenced-code, and TOC support) "
            "or raw HTML. Returns the absolute file path. "
            "Use when the user wants a browser-viewable or printable report."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "content": {
                    "type": "string",
                    "description": "Markdown or raw HTML content to render.",
                },
                "filename": {
                    "type": "string",
                    "description": "Output filename, e.g. 'ad_diagnosis_B0XXXXXX.html'.",
                },
                "title": {
                    "type": "string",
                    "description": "Page <title> and browser tab label (default: 'Report').",
                },
                "is_markdown": {
                    "type": "boolean",
                    "description": "When true (default), content is treated as markdown and converted to HTML.",
                },
                "images": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Optional list of local file paths to upload to the configured storage backend "
                        "(R2 / S3 / MinIO / local-HTTP). Each path is replaced with its public HTTPS URL "
                        "in the content before rendering. Requires STORAGE_BACKEND env vars to be set."
                    ),
                },
            },
            "required": ["content", "filename"],
        },
    )
]
