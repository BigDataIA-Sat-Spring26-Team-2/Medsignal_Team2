import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

try:
    conn = snowflake.connector.connect(
        account  =os.getenv("SNOWFLAKE_ACCOUNT"),
        user     =os.getenv("SNOWFLAKE_USER"),
        password =os.getenv("SNOWFLAKE_PASSWORD"),
        database =os.getenv("SNOWFLAKE_DATABASE"),
        schema   =os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM signals_flagged")
    print("signals_flagged:", cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM safety_briefs")
    print("safety_briefs  :", cur.fetchone()[0])

    cur.execute("SELECT drug_key, pt, prr, stat_score FROM signals_flagged LIMIT 5")
    print("\nSample signals:")
    for row in cur.fetchall():
        print(f"  {row[0]} x {row[1]} | prr={row[2]} stat={row[3]}")

    cur.close()
    conn.close()
    print("\nSnowflake OK")

except Exception as e:
    print("ERROR:", e)