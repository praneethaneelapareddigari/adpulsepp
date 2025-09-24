
-- Schema: ad events
CREATE TABLE IF NOT EXISTS ad_events (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  campaign_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  event_type TEXT NOT NULL CHECK (event_type IN ('impression','click','conversion')),
  cost NUMERIC(12,6) NOT NULL DEFAULT 0,
  revenue NUMERIC(12,6) NOT NULL DEFAULT 0,
  metadata JSONB NOT NULL DEFAULT '{}'
);

-- Indexes for latency-sensitive reports
CREATE INDEX IF NOT EXISTS idx_events_campaign_ts ON ad_events (campaign_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON ad_events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_metadata_gin ON ad_events USING GIN (metadata);

-- Materialized view for hot-window aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_campaign_hourly AS
SELECT
  campaign_id,
  date_trunc('hour', ts) AS hour_bucket,
  count(*) FILTER (WHERE event_type='impression') AS impressions,
  count(*) FILTER (WHERE event_type='click') AS clicks,
  count(*) FILTER (WHERE event_type='conversion') AS conversions,
  sum(cost) AS cost,
  sum(revenue) AS revenue
FROM ad_events
GROUP BY 1,2;

CREATE INDEX IF NOT EXISTS idx_mv_campaign_hourly ON mv_campaign_hourly (campaign_id, hour_bucket DESC);
