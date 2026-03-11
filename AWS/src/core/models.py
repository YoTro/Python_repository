from dataclasses import dataclass, field, asdict
from typing import List, Optional

@dataclass
class StandardProduct:
    asin: str
    title: str = ""
    features: List[str] = field(default_factory=list)
    description: str = ""
    price: Optional[float] = None
    sales_rank: Optional[int] = None
    review_count: Optional[int] = None
    rating: Optional[float] = None
    
    def to_dict(self) -> dict:
        return asdict(self)
