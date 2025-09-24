
import threading, time, json, random, requests, datetime

API = "http://localhost:8080/events"
CAMPAIGNS = ["CAMP-123","CAMP-456","CAMP-789"]
USERS = [f"U{i}" for i in range(1000)]
EVENTS = ["impression","click","conversion"]

def gen_event():
    return {
        "ts": datetime.datetime.utcnow().isoformat()+"Z",
        "campaign_id": random.choice(CAMPAIGNS),
        "user_id": random.choice(USERS),
        "event_type": random.choices(EVENTS, weights=[90,9,1])[0],
        "cost": round(random.random()/100, 6),
        "revenue": round(random.random()/10, 6),
        "metadata": {"geo": random.choice(["IN","US","SG","BR"]),
                     "device": random.choice(["ios","android","web"])}
    }

def worker():
    while True:
        batch = {"events": [gen_event() for _ in range(500)]}
        try:
            r = requests.post(API, json=batch, timeout=5)
            print("inserted", r.json())
        except Exception as e:
            print("err", e)
        time.sleep(0.5)

if __name__ == "__main__":
    for _ in range(4):
        threading.Thread(target=worker, daemon=True).start()
    print("Generating load… Ctrl+C to stop.")
    while True:
        time.sleep(1)
