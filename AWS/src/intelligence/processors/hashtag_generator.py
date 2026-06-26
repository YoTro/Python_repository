from __future__ import annotations

import json
import logging
import re

from src.intelligence.providers.base import BaseLLMProvider

logger = logging.getLogger(__name__)

# Hardcoded TikTok hashtag seeds per product category — supplemental baseline
# alongside LLM-generated competitor hashtags.
_REFERENCE_HASHTAGS: dict[str, list[str]] = {
    "pets": ["pettok", "dogtok", "cattok", "petproducts", "amazonpets"],
    "home": ["homefinds", "cleaninghacks", "homeproducts", "amazonhome", "homehacks"],
    "beauty": ["skincaretok", "beautytok", "makeuptok", "beautyfinds", "amazonskincare"],
    "food": ["foodtok", "cookinghacks", "kitchengadgets", "amazonkitchen", "recipetok"],
    "fitness": ["fitnesstok", "gymtok", "workoutgear", "fitnessproducts", "amazongym"],
    "fashion": ["fashiontok", "ootd", "amazonfashion", "fashionfinds", "styletok"],
    "electronics": ["techtok", "gadgets", "amazontech", "techreview", "gadgetreview"],
    "baby": ["momtok", "babytok", "babyproducts", "newmomtok", "amazonbaby"],
    "toys": ["kidstok", "toysreview", "amazonkids", "toystok", "kidsgifts"],
    "sports": ["sportstok", "outdoortok", "sportsgear", "athletetok", "amazonsports"],
    "office": ["worktok", "officetok", "workfromhome", "desksetup", "amazonoffice"],
}

_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "pets": ["dog", "cat", "pet", "puppy", "kitten", "bird", "fish", "hamster"],
    "home": ["clean", "home", "house", "bug", "insect", "pest", "organiz", "kitchen"],
    "beauty": ["skin", "makeup", "cosmetic", "lip", "serum", "moistur", "foundation"],
    "food": ["food", "cook", "recipe", "snack", "drink", "meal", "bake", "coffee"],
    "fitness": ["gym", "workout", "fitness", "protein", "supplement", "yoga", "pilates"],
    "fashion": ["cloth", "wear", "fashion", "dress", "shoe", "bag", "jewel", "accessory"],
    "electronics": ["tech", "phone", "laptop", "cable", "charger", "gadget", "camera", "usb"],
    "baby": ["baby", "infant", "toddler", "mom", "newborn", "diaper", "stroller"],
    "toys": ["toy", "game", "play", "lego", "puzzle", "doll", "action figure"],
    "sports": ["sport", "outdoor", "camping", "hiking", "cycling", "swim", "tennis"],
    "office": ["desk", "office", "chair", "monitor", "keyboard", "notebook", "pen"],
}


def _infer_category(brand: str, product_name: str) -> str:
    text = f"{brand} {product_name}".lower()
    for cat, keywords in _CATEGORY_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return cat
    return ""


class HashtagGenerator:
    """
    AI-backed processor: generates TikTok reference hashtags for benchmark comparison.

    Combines LLM-generated competitor/category hashtags (primary) with hardcoded
    category seeds (supplemental). Returns a deduplicated list ready for
    tiktok_fetch_reference_data to query.
    """

    def __init__(self, provider: BaseLLMProvider) -> None:
        self.provider = provider

    async def generate_reference_hashtags(
        self,
        brand: str,
        product_name: str,
        keyword: str,
    ) -> list[str]:
        """
        Returns merged, deduplicated hashtag names (without #), excluding the
        target keyword itself. Falls back to hardcoded seeds only on LLM error.
        """
        llm_hashtags = await self._generate_llm_hashtags(brand, product_name, keyword)
        category = _infer_category(brand, product_name)
        hardcoded = _REFERENCE_HASHTAGS.get(category, [])

        return list(
            dict.fromkeys(
                h.lower().lstrip("#")
                for h in llm_hashtags + hardcoded
                if h.lower().lstrip("#") != keyword.lower()
            )
        )

    async def _generate_llm_hashtags(
        self, brand: str, product_name: str, keyword: str
    ) -> list[str]:
        prompt = (
            f"You are a TikTok market research assistant.\n"
            f"Brand: {brand}\nProduct: {product_name}\nTarget hashtag: #{keyword}\n\n"
            f"Suggest 5 TikTok hashtags used by COMPETITOR brands or in the same product category "
            f"that would serve as a fair engagement benchmark for this product.\n"
            f"Include 2-3 direct competitor brand hashtags and 2-3 broad category hashtags.\n"
            f"Return ONLY a JSON array of hashtag names without the # symbol. Example: "
            f'["raidinsecticide", "orthobugs", "bugspray", "pestcontrol", "homepests"]'
        )
        try:
            response = await self.provider.generate_text(prompt)
            text = response.text.strip()
            match = re.search(r"\[.*?\]", text, re.DOTALL)
            if match:
                return json.loads(match.group())
        except Exception as e:
            logger.warning(f"LLM hashtag generation failed: {e}")
        return []
