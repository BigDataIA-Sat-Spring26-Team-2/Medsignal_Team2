"""
app/models/hitl.py — Pydantic schema for HITL decision requests.
"""

from typing import Optional
from pydantic import BaseModel


class HITLDecision(BaseModel):
    drug_key     : str
    pt           : str
    decision     : str          # APPROVE / REJECT / ESCALATE
    reviewer_note: Optional[str] = None
    brief_id     : Optional[int] = None
