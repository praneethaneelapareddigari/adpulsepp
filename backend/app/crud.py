
from datetime import datetime, timedelta
from typing import Optional, List
from psycopg2.extras import execute_values
from .db import get_conn

def insert_events(events: List[dict]) -> int:
    if not events:
        return 0
    q = """INSERT INTO ad_events
    (ts, campaign_id, user_id, event_type, cost, revenue, metadata)
    VALUES %s"""
    rows = [(e['ts'], e['campaign_id'], e['user_id'], e['event_type'], e.get('cost',0), e.get('revenue',0), e.get('metadata',{})) for e in events]
    conn = get_conn()
    cur = conn.cursor()
    execute_values(cur, q, rows, page_size=1000)
    conn.commit()
    cur.close()
    conn.close()
    return len(rows)

def refresh_materialized_view():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_campaign_hourly;")
    conn.commit()
    cur.close()
    conn.close()

def get_report(campaign_id: str, start: Optional[datetime], end: Optional[datetime]):
    conn = get_conn()
    cur = conn.cursor()
    if start is None:
        start = datetime.utcnow() - timedelta(hours=1)
    if end is None:
        end = datetime.utcnow()

    # First try fast path: aggregate via materialized view then patch with delta from latest partial hour
    q_mv = """
    SELECT
      COALESCE(SUM(impressions),0) AS impressions,
      COALESCE(SUM(clicks),0) AS clicks,
      COALESCE(SUM(conversions),0) AS conversions,
      COALESCE(SUM(cost),0) AS cost,
      COALESCE(SUM(revenue),0) AS revenue
    FROM mv_campaign_hourly
    WHERE campaign_id=%s AND hour_bucket >= date_trunc('hour', %s) AND hour_bucket <= date_trunc('hour', %s)
    """
    cur.execute(q_mv, (campaign_id, start, end))
    mv = cur.fetchone() or {'impressions':0, 'clicks':0, 'conversions':0, 'cost':0, 'revenue':0}

    # Delta for the trailing partial hour
    q_delta = """
    SELECT
      COUNT(*) FILTER (WHERE event_type='impression') AS impressions,
      COUNT(*) FILTER (WHERE event_type='click') AS clicks,
      COUNT(*) FILTER (WHERE event_type='conversion') AS conversions,
      SUM(cost) AS cost,
      SUM(revenue) AS revenue
    FROM ad_events
    WHERE campaign_id=%s AND ts >= %s AND ts <= %s
    """
    cur.execute(q_delta, (campaign_id, start, end))
    dl = cur.fetchone() or {'impressions':0, 'clicks':0, 'conversions':0, 'cost':0, 'revenue':0}

    # Combine conservatively (delta overrides overlapping area)
    impressions = dl['impressions'] if dl['impressions'] else mv['impressions']
    clicks = dl['clicks'] if dl['clicks'] else mv['clicks']
    conversions = dl['conversions'] if dl['conversions'] else mv['conversions']
    cost = float(dl['cost'] or mv['cost'] or 0)
    revenue = float(dl['revenue'] or mv['revenue'] or 0)

    cur.close(); conn.close()

    ctr = (clicks / impressions) * 100 if impressions else 0.0
    cvr = (conversions / clicks) * 100 if clicks else 0.0
    roi = ((revenue - cost) / cost) * 100 if cost > 0 else 0.0

    return {
        "impressions": int(impressions),
        "clicks": int(clicks),
        "conversions": int(conversions),
        "cost": round(cost, 6),
        "revenue": round(revenue, 6),
        "ctr": round(ctr, 4),
        "cvr": round(cvr, 4),
        "roi": round(roi, 4),
        "start": start.isoformat(),
        "end": end.isoformat(),
    }
