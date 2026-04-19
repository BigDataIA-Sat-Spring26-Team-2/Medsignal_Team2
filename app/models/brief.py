"""
app/models/brief.py — Pydantic schema for GPT-4o SafetyBrief output validation.
"""

from typing import List, Literal

from pydantic import BaseModel, Field


class SafetyBriefOutput(BaseModel):
    brief_text        : str
    key_findings      : List[str]
    pmids_cited       : List[str]
    recommended_action: Literal["MONITOR", "LABEL_UPDATE", "RESTRICT", "WITHDRAW"]
    drug_key          : str
    pt                : str
    stat_score        : float = Field(ge=0.0, le=1.0)
    lit_score         : float = Field(ge=0.0, le=1.0)
    priority          : Literal["P1", "P2", "P3", "P4"]
    generated_at      : str
