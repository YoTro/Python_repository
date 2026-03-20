import asyncio
import pytest
import os
import json
from pathlib import Path
from src.mcp.servers.amazon.extractors.bsr_category_extractor import BSRCategoryExtractor

# Define a known BSR main URL for testing
BSR_MAIN_URL = "https://www.amazon.com/Best-Sellers/zgbs/"
# Define a known sub-category URL for testing (e.g., Electronics)
BSR_SUB_URL = "https://www.amazon.com/Best-Sellers-Electronics/zgbs/electronics/"
BSR_BOOKS_URL = "https://www.amazon.com/Best-Sellers-Books/zgbs/books/"

@pytest.fixture(scope="function") # Changed scope to function
def extractor():
    """Fixture to provide a BSRCategoryExtractor instance per test."""
    return BSRCategoryExtractor()

@pytest.mark.asyncio
async def test_get_categories_from_page_top_level(extractor: BSRCategoryExtractor):
    """
    Test that get_categories_from_page correctly extracts top-level categories
    from the main BSR page.
    """
    categories = await extractor.get_categories_from_page(BSR_MAIN_URL)
    
    assert categories is not None
    assert len(categories) > 10 # Expect many top-level categories
    
    # Check structure of a few sample categories
    electronics_found = False
    for cat in categories:
        assert "name" in cat
        assert "url" in cat
        assert "node_id" in cat # node_id might be None for some top-level on main page
        # Use a more flexible check for URL start to account for case variations
        assert cat["url"].lower().startswith("https://www.amazon.com/best-sellers-")

        if "Electronics" in cat["name"]:
            electronics_found = True
            # For top-level Electronics on the main page, node_id is typically NOT in the direct URL
            # node_id might be None, which is expected behavior here.
    assert electronics_found, "Electronics category not found in top-level scrape"

@pytest.mark.asyncio
async def test_get_categories_from_page_sub_level(extractor: BSRCategoryExtractor):
    """
    Test that get_categories_from_page correctly extracts sub-level categories
    from a specific category page (e.g., Electronics).
    """
    # Use a well-known category with sub-categories
    sub_categories = await extractor.get_categories_from_page(BSR_SUB_URL)
    
    assert sub_categories is not None
    assert len(sub_categories) > 5 # Expect several sub-categories

    # Check for specific expected sub-categories within Electronics
    expected_sub_categories = ["Accessories & Supplies", "Portable Audio & Video", "Headphones", "Wearable Technology"]
    found_expected = 0
    for cat in sub_categories:
        assert "name" in cat
        assert "url" in cat
        assert cat["url"].startswith("https://www.amazon.com/Best-Sellers-Electronics")
        # Node ID should generally be present for sub-categories
        assert cat["node_id"] is not None 
        if cat["name"] in expected_sub_categories:
            found_expected += 1
            
    assert found_expected >= 2, f"Expected at least 2 sub-categories to be found, got {found_expected}"

@pytest.mark.asyncio
async def test_extract_current_node_id(extractor: BSRCategoryExtractor):
    """
    Test extract_current_node_id with a sample HTML snippet.
    """
    # Sample HTML snippet (simplified) containing browseNodeId in script tag
    sample_html_script = """
    <html><body>
        <script type="text/javascript">
            var data = {
                "something": "value",
                "browseNodeId": "1234567890",
                "another": "field"
            };
        </script>
    </body></html>
    """
    node_id = extractor.extract_current_node_id(sample_html_script)
    assert node_id == "1234567890"

    # Sample HTML snippet (simplified) containing data-node-id
    sample_html_data_attr = """
    <html><body>
        <a href="/some/path" data-node-id="9876543210">Link</a>
    </body></html>
    """
    node_id = extractor.extract_current_node_id(sample_html_data_attr)
    assert node_id == "9876543210"

    # Test with no node ID found
    sample_html_no_id = "<html><body>No ID here</body></html>"
    node_id = extractor.extract_current_node_id(sample_html_no_id)
    assert node_id is None

@pytest.mark.asyncio
async def test_get_categories_from_page_books(extractor: BSRCategoryExtractor):
    """
    Test with a category known to sometimes have different sidebar structure (Books).
    """
    categories = await extractor.get_categories_from_page(BSR_BOOKS_URL)
    
    assert categories is not None
    assert len(categories) > 5 # Expect several sub-categories

    # Check for specific expected sub-categories within Books
    expected_sub_categories = ["Literature & Fiction", "Mystery, Thriller & Suspense", "Romance", "Science Fiction & Fantasy"]
    found_expected = 0
    for cat in categories:
        if cat["name"] in expected_sub_categories:
            found_expected += 1
    
    assert found_expected >= 2, f"Expected at least 2 book sub-categories, got {found_expected}"

