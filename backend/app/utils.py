
from datetime import datetime, timedelta
def parse_relative_window(s: str):
    # accepts patterns like -1h, -24h, -7d
    now = datetime.utcnow()
    if not s or not s.startswith('-'):
        return None
    unit = s[-1]
    val = int(s[1:-1])
    if unit == 'h':
        return now - timedelta(hours=val)
    if unit == 'd':
        return now - timedelta(days=val)
    return None
