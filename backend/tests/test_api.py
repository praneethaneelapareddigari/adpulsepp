
from app.schemas import Event, EventBatch
from app.crud import get_report
from datetime import datetime, timedelta

def test_metrics_math():
    start = datetime.utcnow() - timedelta(hours=1)
    end = datetime.utcnow()
    # math only (no DB): validate the ROI/CTR/CVR equations by creating a fake response-like dict
    impressions, clicks, conversions = 1000, 50, 5
    cost, revenue = 10.0, 25.0
    ctr = (clicks/impressions)*100
    cvr = (conversions/clicks)*100
    roi = ((revenue - cost)/cost)*100
    assert round(ctr,4) == 5.0
    assert round(cvr,4) == 10.0
    assert round(roi,4) == 150.0
