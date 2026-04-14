
import psycopg2, os
from dotenv import load_dotenv
load_dotenv()
print('HOST:', os.getenv('POSTGRES_HOST'))
print('DB:', os.getenv('POSTGRES_DB'))
try:
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        sslmode='require'
    )
    print('Connected OK')
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM signals_flagged')
    print('Total:', cur.fetchone()[0])
    cur.execute('SELECT drug_key, pt, prr, stat_score FROM signals_flagged LIMIT 5')
    for row in cur.fetchall():
        print(row)
    conn.close()
except Exception as e:
    print('ERROR:', e)

