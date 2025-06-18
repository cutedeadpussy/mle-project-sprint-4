import logging
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename="test.log",
    filemode="a",
    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.DEBUG,
)

def log_response(resp):
    if resp.status_code == 200:
        logging.info(resp.json())
        return resp.json()
    else:
        logging.info(f"status code: {resp.status_code}")
        return []

headers = {"Content-type": "application/json", "Accept": "text/plain"}

recommendations_url = "http://127.0.0.1:8000/recommendations"
params = {"user_id": 47}
recs = log_response(requests.post(recommendations_url, headers=headers, params=params))
logging.info(f"Полученные идентификаторы рекомендаций: {recs}")

params = {"user_id": 6666666}
recs = log_response(requests.post(recommendations_url, headers=headers, params=params))
logging.info(f"Полученные идентификаторы рекомендаций: {recs}")

load_recommendations_url = "http://127.0.0.1:8000/load_recommendations"
params = {"rec_type": "default", "file_path": "top_popular.parquet"}
logging.info(f"status_code: {requests.get(load_recommendations_url, headers=headers, params=params).status_code}")

get_statistics_url = "http://127.0.0.1:8000/get_statistics"
log_response(requests.get(get_statistics_url))

get_user_events_url = "http://127.0.0.1:8000/get_user_events"
params = {"user_id": 69, "k": 10}
log_response(requests.post(get_user_events_url, headers=headers, params=params))

put_user_events_url = 'http://127.0.0.1:8000/put_user_event'
for i in [679699, 630670, 646516, 19152669, 38646012]:
    print(i)
    params = {"user_id": 69, "item_id": i}
    log_response(requests.post(put_user_events_url, headers=headers, params=params))

params = {"user_id": 69, "k": 10}
log_response(requests.post(get_user_events_url, headers=headers, params=params))

get_online_rec_url = 'http://127.0.0.1:8000/get_online_rec'
params = {"user_id": 69, "k": 100, "N": 10}
log_response(requests.post(get_online_rec_url, headers=headers, params=params))

params = {"user_id": 69}
recs = log_response(requests.post(recommendations_url, headers=headers, params=params))
logging.info(f"Полученные идентификаторы рекомендаций: {recs}")
