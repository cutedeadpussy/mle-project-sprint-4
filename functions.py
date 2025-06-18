import os
import io
import logging
import pandas as pd
from dotenv import load_dotenv
import boto3
from implicit.als import AlternatingLeastSquares

load_dotenv()

logger = logging.getLogger("uvicorn.error")

# Подключение к S3
session = boto3.session.Session()
s3 = session.client(
    service_name="s3",
    endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
)

BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

# Загрузка модели ALS
als_model = AlternatingLeastSquares(factors=50, iterations=50, regularization=0.05, random_state=0)
obj_als_model = s3.get_object(Bucket=BUCKET_NAME, Key=os.environ.get("KEY_ALS_MODEL"))
als_model = als_model.load(io.BytesIO(obj_als_model["Body"].read()))

# Загрузка items.parquet
obj_items = s3.get_object(Bucket=BUCKET_NAME, Key=os.environ.get("KEY_ITEMS_PARQUET"))
items = pd.read_parquet(io.BytesIO(obj_items["Body"].read()))

print(items.columns)
# Класс рекомендаций
class Recommendations:
    def __init__(self):
        self._recs = {"personal": None, "default": None}
        self._stats = {"request_personal_count": 0, "request_default_count": 0}

    def load(self, type: str, path: str, **kwargs):
        logger.info(f"Loading recommendations, type: {type}")
        key_env = "KEY_PERSONAL_ALS_PARQUET" if type == "personal" else "KEY_TOP_POPULAR_PARQUET"
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=os.environ.get(key_env))
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()), **kwargs)

        self._recs[type] = df.set_index("user_id") if type == "personal" else df
        logger.info("Loaded")

    def get(self, user_id: int, k: int = 100):
        try:
            recs = self._recs["personal"].loc[user_id]["track_id"].tolist()[:k]
            self._stats["request_personal_count"] += 1
            logger.info(f"Found {len(recs)} personal recommendations!")
        except Exception:
            recs = self._recs["default"]["track_id"].tolist()[:k]
            self._stats["request_default_count"] += 1
            logger.info(f"Found {len(recs)} TOP-recommendations!")

        if not recs:
            logger.error("No recommendations found")
        return recs or []

    def stats(self):
        logger.info("Stats for recommendations")
        for k, v in self._stats.items():
            logger.info(f"{k:<30} {v}")
        print(self._stats)
        return self._stats

# Класс хранения событий
class EventStore:
    def __init__(self, max_events_per_user: int = 10):
        self.events = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id: int, item_id: int):
        self.events[user_id] = [item_id] + self.events.get(user_id, [])[:self.max_events_per_user]

    def get(self, user_id: int, k: int):
        return self.events.get(user_id, [])[:k]

rec_store = Recommendations()
events_store = EventStore()

# Поиск похожих треков через ALS
async def als_sim(track_id: int, N: int = 1):
    track_row = items.loc[items["track_id"] == track_id]
    if track_row.empty:
        return [], []

    print(items.columns)
    track_id_enc = track_row["track_id_enc"].iloc[0]
    similar_items = als_model.similar_items(track_id_enc, N=N)
    enc_ids, scores = similar_items[0][1:N+1], similar_items[1][1:N+1]

    similar_tracks = items[items["track_id_enc"].isin(enc_ids)]["track_id"].tolist()
    return similar_tracks, scores
