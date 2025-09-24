
import json
from datetime import datetime

def handler(event, context):
    # event from SQS with Records, each body is an event JSON
    events = []
    for r in event.get('Records', []):
        body = json.loads(r['body'])
        body.setdefault('ts', datetime.utcnow().isoformat()+'Z')
        events.append(body)

    # In real deploy, post to the FastAPI /events in batches
    # (left as stub to avoid network calls in sample)
    return {"received": len(events)}
