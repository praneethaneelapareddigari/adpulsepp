
import psycopg2
from psycopg2.extras import RealDictCursor
from .config import settings

def get_conn():
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        dbname=settings.DB_NAME,
        cursor_factory=RealDictCursor
    )

def migrate():
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    with open('/app/migrations/001_init.sql','r') as f:
        cur.execute(f.read())
    cur.close()
    conn.close()
