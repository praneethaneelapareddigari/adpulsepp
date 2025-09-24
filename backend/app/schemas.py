
from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Dict, Any
from datetime import datetime

EventType = Literal['impression','click','conversion']

class Event(BaseModel):
    ts: datetime
    campaign_id: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    event_type: EventType
    cost: float = 0.0
    revenue: float = 0.0
    metadata: Dict[str, Any] = {}

class EventBatch(BaseModel):
    events: List[Event]

class Report(BaseModel):
    campaign_id: str
    start: Optional[str] = None
    end: Optional[str] = None
    impressions: int
    clicks: int
    conversions: int
    ctr: float
    cvr: float
    cost: float
    revenue: float
    roi: float
