"""
agent3_assessor.py — Agent 3: Priority Tier Assignment + SafetyBrief Generation

Receives state populated by Agent 1 (stat_score) and Agent 2 (abstracts, lit_score).
Assigns a P1-P4 priority tier, calls GPT-4o to synthesise a SafetyBrief,
validates output with Pydantic, strips fabricated PMIDs, and writes to Snowflake.

Retry logic: one retry on Pydantic failure with the validation error in prompt.
On second failure: writes generation_error=True so HITL still sees the signal.

Owner: Siddharth
"""
import json
import logging
import math
import os
from datetime import datetime, timezone
from typing import List, Literal, Optional

import snowflake.connector
from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field, ValidationError

from app.agents.state import SignalState

load_dotenv()

log    = logging.getLogger(__name__)
client = OpenAI()
MODEL  = "gpt-4o"
