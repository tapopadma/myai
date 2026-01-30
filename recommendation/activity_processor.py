import argparse, json, os, psycopg2, time

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from google import genai


KAFKA_INSTANCE = "localhost:9092"
KAFKA_TOPIC = "health-activities"
CONSUMER_GROUP = "activity-processor"
HEALTH_ADVICE_FEED_PATH = "../model/feed/health_advice_train.jsonl"

PARSER = argparse.ArgumentParser(description="activity processor binary flags.")
PARSER.add_argument('--user', type=str, help='db user')
PARSER.add_argument('--pwd', type=str, help='db password')
PARSER.add_argument('--db', type=str, help='db name')
ARGS = PARSER.parse_args()

DB_PARAMS = {
    "dbname": ARGS.db,
    "user": ARGS.user,
    "password": ARGS.pwd,
    "host": "localhost"
}

class HealthAdviceRAG:
    def __init__(self, feed_path: str):
        self.docs = []

        with open(feed_path, "r") as f:
            for line in f:
                record = json.loads(line)
                text = f"Problem: {record.get('instruction', '')} Advice: {record.get('output', '')}"
                self.docs.append(text)

        self.vectorizer = TfidfVectorizer(stop_words="english")
        self.embeddings = self.vectorizer.fit_transform(self.docs)

    def retrieve(self, query: str, top_k: int = 3) -> list[str]:
        query_vec = self.vectorizer.transform([query])
        scores = cosine_similarity(query_vec, self.embeddings)[0]

        top_indices = scores.argsort()[-top_k:][::-1]
        return [self.docs[i] for i in top_indices]


class ActivityProcessor:

    def __init__(self):
        self._consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_INSTANCE,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=False, #each message is critical
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            value_deserializer=self._safe_json_deserializer
        )

        self.connection = psycopg2.connect(**DB_PARAMS)
        self.cursor = self.connection.cursor()

        self._rag = HealthAdviceRAG(HEALTH_ADVICE_FEED_PATH)
        
        if not os.environ.get("GOOGLE_API_KEY"):
            raise Exception("API key missing for Google Gemini")
        self._model = genai.Client(api_key=os.environ["GOOGLE_API_KEY"])

    def _safe_json_deserializer(self, v):
        if v is None:
            return None
        try:
            return json.loads(v.decode("utf-8"))
        except json.JSONDecodeError:
            print(f"skipping non-json message: {v!r}")
            return None

    def _save_recommendation(self, content):
        print(f"Attempting recommendation write with content:\n{content}\n=====\n")
        try:
            insert_query = """
                INSERT INTO Recommendations (content,status) 
                VALUES (%s,%s);
            """
            self.cursor.execute(insert_query, (content,'CREATED'))
            self.connection.commit()
            print("Recommendation write successful.")
        except Exception as e:
            self.connection.rollback()
            raise e

    def _build_user_query(self, activities):
        return f"Find best matching advises based on the following user activities:\n\n{activities}"

    def _extract_activities(self, activity_summary):
        activities = ""
        for activity in activity_summary['activities']['activities']:
            activities += f"For {activity['type']} activity in {activity['source_app']} app, "
            activities += activity['summary'] + "\n\n"
        return activities

    def _build_recommendation(self, message):
        activities = self._extract_activities(message.value)
        query = self._build_user_query(activities)
        print(f"received query: {query}")
        context_docs = self._rag.retrieve(query)
        context = "\n\n".join(context_docs)

        prompt = f"""
        You're a health advisor. Based primarily on pre-recommended medical guidelines and the 
        recent activities of the user 
        in their personal device, advice a few health or physical activity
        tips so that can be notified to the user in their device.

        Use the following medical guideline as reference only.
        Answer in short to fit as a typical notification.
        Do NOT provide a diagnosis.
        If uncertain, say so and recommend seeing a healthcare professional.

        Recommended Medical guidelines:
        {context}

        User recent activities:
        {activities}
        """

        print(f"Sending prompt > {prompt}.")

        response = self._model.models.generate_content(
            model="gemini-flash-latest",
            contents=prompt,
            config={
                "temperature": 0.2,
                "max_output_tokens": 600 #good enough for a notification text
            }
        )

        return response.text

    def _process_activity(self, message):
        self._save_recommendation(self._build_recommendation(message))

    def listen(self):
        print("Listening to activity message queue for new activities...")
        for message in self._consumer:
            if message.value is None:
                print(f"Skipping invalid message at offset {message.offset}")
                self._commit_message(message)
                continue
            success = False
            delay = 5
            for attempt in range(6):
                try:
                    print(f"processing message {message}")
                    self._process_activity(message)
                    success = True
                    break
                except Exception as e:
                    # trigger inline retry simply.
                    print(f"Attempt #{attempt} failed: {e}")
                    print(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    delay*=2
            if not success:
                print(f"CRITICAL: Failed to process message at offset {message.offset} after all retries.")
            print(f"Message processed successfully at offset {message.offset}.")
            self._consumer.commit()
            print(f"Message committed successfully at offset {message.offset}.")

ActivityProcessor().listen()