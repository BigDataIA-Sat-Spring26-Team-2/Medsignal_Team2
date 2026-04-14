import os
import pandas as pd
import snowflake.connector
from sqlalchemy import create_engine
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Read from Supabase using SQLAlchemy (fixes pandas warning)
engine = create_engine(
    "postgresql+psycopg2://",
    creator=lambda: psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        sslmode="require"
    )
)
df = pd.read_sql("SELECT prod_ai, rxcui, canonical_name FROM rxnorm_cache", con=engine)
engine.dispose()

# Replace NaN with None so Snowflake inserts NULL not 'NAN'
df = df.where(pd.notnull(df), None)

print(f"Read {len(df)} rows from Supabase")

# Write to Snowflake
sf_conn = snowflake.connector.connect(
    account  = os.getenv("SNOWFLAKE_ACCOUNT"),
    user     = os.getenv("SNOWFLAKE_USER"),
    password = os.getenv("SNOWFLAKE_PASSWORD"),
    database = os.getenv("SNOWFLAKE_DATABASE"),
    schema   = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
    warehouse= os.getenv("SNOWFLAKE_WAREHOUSE"),
)
cur = sf_conn.cursor()

# Replace NaN more explicitly
def clean_val(v):
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    return v

rows = [
    (clean_val(row["prod_ai"]), clean_val(row["rxcui"]), clean_val(row["canonical_name"]))
    for _, row in df.iterrows()
]

cur.executemany(
    "INSERT INTO rxnorm_cache (prod_ai, rxcui, canonical_name) VALUES (%s, %s, %s)",
    rows
)

sf_conn.commit()
cur.close()
sf_conn.close()

print(f"Migrated {len(df)} rows to Snowflake rxnorm_cache")