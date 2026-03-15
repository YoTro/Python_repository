from __future__ import annotations
import os
import json
import logging
from typing import List
from mcp.types import Resource

logger = logging.getLogger("mcp-resources")

class ResourceRegistry:
    """
    Centralized registry for MCP resources.
    Automatically discovers and exposes business knowledge from JSON files.
    """
    def __init__(self, base_dir: str = None):
        if base_dir is None:
            # Default to scanning all servers directories
            base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "servers")
        self.base_dir = base_dir
        self._resource_map = {} # Mapping of URI to local file path

    def get_all_resources(self) -> List[Resource]:
        """
        Scan the servers directory recursively and return a list of MCP Resource objects.
        """
        resources = []
        try:
            for root, dirs, files in os.walk(self.base_dir):
                for filename in files:
                    if filename.endswith(".json"):
                        name = filename.replace(".json", "").replace("_", " ").title()
                        uri = f"resource://aws-knowledge/{filename}"
                        
                        resources.append(Resource(
                            uri=uri,
                            name=f"Amazon {name} Reference",
                            description=f"Static business data for {name.lower()} logic.",
                            mimeType="application/json"
                        ))
                        # Map URI to physical path for reading later
                        self._resource_map[uri] = os.path.join(root, filename)
        except Exception as e:
            logger.error(f"Error scanning resources: {e}")
            
        return resources

    def read_resource(self, uri: str) -> str:
        """
        Read the content of a registered resource.
        """
        file_path = self._resource_map.get(uri)
        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError(f"Resource with URI {uri} not found.")
            
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()

# Singleton instance
resource_registry = ResourceRegistry()
