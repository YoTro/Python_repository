import asyncio
import os
import sys

# Ensure src is in path for dynamic imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import src
from src.mcp.servers.social.tiktok.client import TikTokClient

async def test():
    client = TikTokClient()
    video_id = "7620042289246768404"
    print(f"Testing get_video_comments for video: {video_id}")
    comments = client.get_video_comments(video_id, count=100)
    
    if comments:
        print(f"Success! Retrieved {len(comments)} comments.")
        for i, c in enumerate(comments):
            print(f"{i+1}. {c.get('text')}")
    else:
        print("No comments retrieved.")

if __name__ == "__main__":
    asyncio.run(test())
