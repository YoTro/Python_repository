import json
import logging
import os
import re
from typing import List, Dict, Any, Optional
from mcp.types import Tool, TextContent
from src.registry.tools import tool_registry

logger = logging.getLogger("mcp-compliance")

# --- Helper Functions ---

def load_json(filename: str) -> Dict[str, Any]:
    try:
        base_path = os.path.dirname(__file__)
        path = os.path.join(base_path, filename)
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load compliance data {filename}: {e}")
        return {}

def keyword_match(keyword: str, text_list: List[str]) -> bool:
    if not keyword:
        return False
    pattern = re.compile(re.escape(keyword), re.IGNORECASE)
    for text in text_list:
        if pattern.search(text):
            return True
    return False

# --- Core Logic ---

async def handle_compliance_tool(name: str, arguments: dict) -> list[TextContent]:
    amazon_data = load_json("amazon_restricted_products.json")
    epa_data = load_json("epa_pesticide_devices.json")

    if name == "check_epa":
        keyword = arguments.get("keyword", "").lower()
        results = []
        
        # Check EPA Specific Data
        for cat in epa_data.get("regulated_device_categories", []):
            if keyword in cat["category"].lower() or any(keyword in ex.lower() for ex in cat.get("examples", [])):
                results.append({
                    "type": "EPA Regulated Device",
                    "category": cat["category"],
                    "description": cat["description"],
                    "conditions": cat.get("conditions")
                })
        
        for item in epa_data.get("not_devices_commonly_mistaken", []):
            if keyword in item["product_type"].lower():
                results.append({
                    "type": "EPA Registered Pesticide (NOT Device)",
                    "product_type": item["product_type"],
                    "reason": item["reason"],
                    "regulation": item["regulation"]
                })

        if not results:
            return [TextContent(type="text", text=json.dumps({"status": "clean", "message": f"No specific EPA pesticide device restrictions found for '{keyword}' in local database."}, indent=2, ensure_ascii=False))]
        
        return [TextContent(type="text", text=json.dumps({"status": "warning", "findings": results}, indent=2, ensure_ascii=False))]

    elif name == "check_amazon_restriction":
        keyword = arguments.get("keyword", "").lower()
        findings = []
        
        for cat in amazon_data.get("restricted_categories", []):
            # Match category name or any prohibited/allowed examples
            match = False
            if keyword in cat["name"].lower():
                match = True
            elif any(keyword in p.lower() for p in cat.get("prohibited", [])):
                match = True
            elif any(keyword in a.lower() for a in cat.get("allowed", [])):
                match = True
            
            if match:
                findings.append({
                    "category": cat["name"],
                    "approval_required": cat["approval_required"],
                    "prohibited_examples": [p for p in cat.get("prohibited", []) if keyword in p.lower()][:3],
                    "seller_central_link": cat.get("seller_central_ref")
                })

        if not findings:
            return [TextContent(type="text", text=json.dumps({"status": "pass", "message": "No direct matches in Amazon restricted categories."}, indent=2, ensure_ascii=False))]
        
        return [TextContent(type="text", text=json.dumps({"status": "restricted_or_flagged", "findings": findings}, indent=2, ensure_ascii=False))]

    elif name == "get_regulations":
        category_query = arguments.get("category", "").lower()
        regulations = []
        
        for cat in amazon_data.get("restricted_categories", []):
            if category_query in cat["name"].lower():
                regulations.append({
                    "category": cat["name"],
                    "approval_required": cat["approval_required"],
                    "allowed_summary": cat.get("allowed", [])[:5],
                    "prohibited_summary": cat.get("prohibited", [])[:5],
                    "ref": cat.get("seller_central_ref")
                })
        
        if not regulations:
            return [TextContent(type="text", text=json.dumps({"message": f"No regulation info found for category: {category_query}"}, indent=2, ensure_ascii=False))]
            
        return [TextContent(type="text", text=json.dumps({"regulations": regulations}, indent=2, ensure_ascii=False))]

    elif name == "check_patent":
        # Simplified patent risk check based on known prohibited items in restricted categories
        # (e.g. products defeating emissions, odometer rollback, etc.)
        keyword = arguments.get("keyword", "").lower()
        findings = []
        
        # Look for keywords that often imply legal/patent/IP risk in the prohibited list
        for cat in amazon_data.get("restricted_categories", []):
            for p in cat.get("prohibited", []):
                if keyword in p.lower() and ("copyright" in p.lower() or "trademark" in p.lower() or "unlicensed" in p.lower() or "counterfeit" in p.lower()):
                    findings.append({
                        "risk_type": "IP/Copyright/Trademark",
                        "context": p,
                        "category": cat["name"]
                    })

        status = "high" if findings else "low"
        return [TextContent(type="text", text=json.dumps({"risk_level": status, "findings": findings}, indent=2, ensure_ascii=False))]

    return [TextContent(type="text", text=f"Unknown tool: {name}")]

# --- Tool Definitions ---

compliance_tools = [
    Tool(
        name="check_epa",
        description="Verify if a product (e.g., UV lamp, air purifier, pesticide) is regulated by the EPA under FIFRA using the local compliance database.",
        inputSchema={
            "type": "object", 
            "properties": {
                "keyword": {"type": "string", "description": "The product type or keyword to check (e.g., 'UV', 'pesticide', 'filter')"}
            }, 
            "required": ["keyword"]
        }
    ),
    Tool(
        name="check_amazon_restriction",
        description="Check if a product falls under any Amazon restricted product categories.",
        inputSchema={
            "type": "object", 
            "properties": {
                "keyword": {"type": "string", "description": "The product name or keyword (e.g., 'alcohol', 'knife', 'supplement')"}
            }, 
            "required": ["keyword"]
        }
    ),
    Tool(
        name="check_patent",
        description="Perform a basic risk assessment for patent and intellectual property violations based on prohibited product examples.",
        inputSchema={
            "type": "object", 
            "properties": {
                "keyword": {"type": "string", "description": "Product keyword to check for IP/counterfeit risk"}
            }, 
            "required": ["keyword"]
        }
    ),
    Tool(
        name="get_regulations",
        description="Retrieve detailed regulatory requirements and Seller Central references for a specific Amazon product category.",
        inputSchema={
            "type": "object", 
            "properties": {
                "category": {"type": "string", "description": "The category name (e.g., 'Electronics', 'Medical Devices')"}
            }, 
            "required": ["category"]
        }
    )
]

_COMPLIANCE_META = {
    "check_epa": ("FILTER", "EPA regulation findings"),
    "check_amazon_restriction": ("FILTER", "Amazon restriction findings"),
    "check_patent": ("FILTER", "IP risk assessment"),
    "get_regulations": ("FILTER", "detailed category regulations"),
}

for tool in compliance_tools:
    cat, ret = _COMPLIANCE_META.get(tool.name, ("FILTER", ""))
    tool_registry.register_tool(tool, handle_compliance_tool, category=cat, returns=ret)
