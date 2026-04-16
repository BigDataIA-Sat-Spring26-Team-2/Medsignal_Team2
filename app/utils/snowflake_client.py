"""
snowflake_client.py — Shared Snowflake connection factory.

Used by branch2_prr.py, agent3_assessor.py, pipeline.py, and api/main.py.
Credentials come from SNOWFLAKE_* env vars.
"""

import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def get_conn() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account  =os.getenv("SNOWFLAKE_ACCOUNT"),
        user     =os.getenv("SNOWFLAKE_USER"),
        password =os.getenv("SNOWFLAKE_PASSWORD"),
        database =os.getenv("SNOWFLAKE_DATABASE"),
        schema   =os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )