import json, os

from datetime import datetime
from zoneinfo import ZoneInfo
from kafka import KafkaProducer

KAFKA_INSTANCE = "localhost:9092"
KAFKA_TOPIC = "health-activities"
TRACKER_FILE = "./feed/extraction_tracker.json"
ACTIVITY_DIR = "./feed"
ACTIVITY_FILE = "activity.json"

class ActivityExtractor:

    def __init__(self):
        self._extractor = KafkaProducer(
            bootstrap_servers=KAFKA_INSTANCE,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3
        )

    def __del__(self):
        self._extractor.close()

    def _fetch_last_extracted(self):
        try:
            with open(TRACKER_FILE, "r") as f:
                data = json.load(f)
            return data.get("last_successful_extraction_date")
        except Exception as e:
            print(f"Tracker file read failure: {e}")
            return ""

    def _fetch_new_activities(self):
        # Read all new activities.
        last_processed = self._fetch_last_extracted()
        print(f"Last processed on: {last_processed}")
        messages = []
        for root, dirs, _ in os.walk(ACTIVITY_DIR):
            for dir in dirs:
                if datetime.strptime(dir, "%Y-%m-%d") <= datetime.strptime(last_processed, "%Y-%m-%d"):
                    continue
                file_path = os.path.join(root, dir, ACTIVITY_FILE)
                try:
                    with open(file_path, "r") as f:
                        activities = json.load(f)

                    messages.append({
                        "file_path": file_path,
                        "partition": dir,
                        "activities": activities
                    })
                except Exception as e:
                    print(f"activity file parse failure {file_path}: {e}")
        print(f"Fetched {len(messages)} new activities")
        return messages

    def _push_to_activity_queue(self, messages):
        for msg in messages:
            self._extractor.send(
                topic=KAFKA_TOPIC,
                key=msg["file_path"],
                value={
                    "event_type": "ACTIVITY_FILE",
                    "timestamp": msg["partition"],
                    "activities": msg["activities"]
                }
            )
            print(f"Pushed: {msg}")
        self._extractor.flush()

    def _update_tracker(self):
        today_date = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d")
        with open(TRACKER_FILE, "w") as f:
            json.dump(
                {
                    "last_successful_extraction_date": today_date
                },
                f,
                indent=2
            )

    def extract(self):
        pending_messages = self._fetch_new_activities()
        self._push_to_activity_queue(pending_messages)
        self._update_tracker()

ActivityExtractor().extract()
