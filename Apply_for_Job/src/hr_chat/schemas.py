"""
schemas.py - DTOs for the hr_chat module

All dataclasses are plain Python (no external deps) so they can be imported
anywhere in the project without pulling in anthropic/pandas.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class JobSnapshot:
    """
    Minimal view of a job posting passed into an HR chat session.
    Constructed from a normalizer.py output row before calling run_session().
    """
    job_title:    str
    company:      str
    salary_raw:   Optional[str] = None
    location:     Optional[str] = None
    description:  Optional[str] = None
    source:       Optional[str] = None   # "51job" | "zhipin"
    url:          Optional[str] = None

    @classmethod
    def from_series(cls, row) -> "JobSnapshot":
        """Build from a pandas Series (normalizer output row)."""
        return cls(
            job_title   = str(row.get("job_title",  "") or ""),
            company     = str(row.get("company",    "") or ""),
            salary_raw  = row.get("salary_raw")  or None,
            location    = row.get("location")    or None,
            description = row.get("description") or None,
            source      = row.get("source")      or None,
            url         = row.get("url")         or None,
        )


@dataclass
class ChatTurn:
    """A single question-answer pair in an HR conversation."""
    question: str
    answer:   str


@dataclass
class HrChatResult:
    """
    Output of one complete HR chat session.

    Extracted fields (all optional — only populated when HR actually answers):
      - category         : e.g. "服装", "3C", "家居"  (Amazon category)
      - avg_order_value  : customer average order value (USD or CNY, raw string)
      - team_size        : number of people on the operations team
      - marketplace      : e.g. "美国站", "欧洲站", "全球"
      - monthly_sales    : monthly GMV / revenue target (raw string)
      - brand_type       : "自有品牌" / "白牌" / "分销" / "OEM"
      - tools_used       : list of tools/software required (Helium10, SP, etc.)
      - work_mode        : "remote" / "hybrid" / "onsite"
      - extra            : catch-all dict for any other extracted key-value pairs
    """
    job:        JobSnapshot
    turns:      list[ChatTurn]   = field(default_factory=list)
    raw_summary: Optional[str]   = None   # final Claude summary of session

    # --- structured extractions ---
    category:        Optional[str]       = None
    avg_order_value: Optional[str]       = None
    team_size:       Optional[int]       = None
    marketplace:     Optional[str]       = None
    monthly_sales:   Optional[str]       = None
    brand_type:      Optional[str]       = None
    tools_used:      list[str]           = field(default_factory=list)
    work_mode:       Optional[str]       = None
    extra:           dict                = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Flat dict for merging back into a DataFrame row."""
        return {
            "hrc_category":        self.category,
            "hrc_avg_order_value": self.avg_order_value,
            "hrc_team_size":       self.team_size,
            "hrc_marketplace":     self.marketplace,
            "hrc_monthly_sales":   self.monthly_sales,
            "hrc_brand_type":      self.brand_type,
            "hrc_tools_used":      ", ".join(self.tools_used) if self.tools_used else None,
            "hrc_work_mode":       self.work_mode,
            "hrc_raw_summary":     self.raw_summary,
            **{f"hrc_extra_{k}": v for k, v in self.extra.items()},
        }
