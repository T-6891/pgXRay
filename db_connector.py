import psycopg2
import psycopg2.extras
from typing import Dict, List, Any, Tuple

class DatabaseConnector:
    def __init__(self, conn_str: str):
        self.conn = psycopg2.connect(conn_str)
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    def fetch_all(self, sql: str, params=None) -> List[Dict]:
        self.cur.execute(sql, params or ())
        return self.cur.fetchall()
    
    def fetch_one(self, sql: str, params=None) -> Dict:
        self.cur.execute(sql, params or ())
        return self.cur.fetchone()
    
    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
