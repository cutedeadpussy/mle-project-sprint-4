from functions import rec_store, events_store, items, als_sim
from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging
from dotenv import load_dotenv

load_dotenv()

# Используем логгер uvicorn для логирования всех сообщений
logger = logging.getLogger("uvicorn.error")


# Жизненный цикл приложения: загружаем данные при старте и освобождаем ресурсы при завершении
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup")

    rec_store.load(
        type="personal",
        path="personal_als.parquet",
        columns=["user_id", "track_id", "score"],
    )
    rec_store.load(
        type="default",
        path="top_popular.parquet",
        columns=["track_id", "popularity_weighted"],
    )

    yield
    logger.info("Application shutdown")


# Инициализация FastAPI
app = FastAPI(title="Recommendation Microservice", lifespan=lifespan)


@app.post("/recommendations", name="Get user recommendations")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает объединённые рекомендации (онлайн + офлайн) длиной k для пользователя
    """

    recs_offline = rec_store.get(user_id, k)
    recs_online = (await get_online_rec(user_id, k, N=10))["recs"]

    logger.info(f"Offline recs: {recs_offline}")
    logger.info(f"Online recs: {recs_online}")

    # Смешиваем рекомендации, чередуя офлайн и онлайн
    blended = [
        val
        for pair in zip(recs_online, recs_offline)
        for val in pair
    ]
    blended.extend(recs_online[len(recs_offline):])
    blended.extend(recs_offline[len(recs_online):])

    # Удаляем дубликаты, сохраняем порядок и ограничиваем длину
    recs_blended = list(dict.fromkeys(blended))[:k]

    # Отображаем названия и артистов по рекомендациям
    for track_id in recs_blended:
        try:
            row = items.loc[items["track_id"] == track_id]
            print("Track:", row["track_name"].values[0])
            print("Artist:", row["artist_name"].values[0])
        except IndexError:
            logger.warning(f"Track ID {track_id} not found in items")

    return {"recs": recs_blended}


@app.post("/get_online_rec")
async def get_online_rec(user_id: int, k: int = 100, N: int = 10):
    """
    Генерирует онлайн-рекомендации на основе последних действий пользователя
    """

    events = (await get_user_events(user_id, k))["events"]

    sim_ids, sim_scores = [], []
    for track_id in events:
        ids, scores = await als_sim(track_id, N)
        sim_ids.extend(ids)
        sim_scores.extend(scores)

    # Сортируем и убираем дубликаты
    combined = sorted(zip(sim_ids, sim_scores), key=lambda x: x[1], reverse=True)
    recs = list(dict.fromkeys([track_id for track_id, _ in combined]))

    for track_id in recs:
        try:
            row = items.loc[items["track_id"] == track_id]
            print("Track:", row["track_name"].values[0])
            print("Artist:", row["artist_name"].values[0])
        except IndexError:
            logger.warning(f"Track ID {track_id} not found in items")

    return {"recs": recs}


@app.post("/put_user_event")
async def put_user_event(user_id: int, item_id: int):
    """
    Сохраняет действие пользователя (прослушивание трека)
    """
    events_store.put(user_id, item_id)
    return {"result": "ok"}


@app.post("/get_user_events")
async def get_user_events(user_id: int, k: int = 10):
    """
    Возвращает последние k событий пользователя
    """
    return {"events": events_store.get(user_id, k)}


@app.get("/load_recommendations", name="Reload recommendation files")
async def load_recommendations(rec_type: str, file_path: str):
    """
    Позволяет вручную перезагрузить офлайн-рекомендации из указанного файла
    """
    columns = ["user_id", "track_id", "score"] if rec_type == "personal" else ["track_id", "popularity_weighted"]
    rec_store.load(type=rec_type, path=file_path, columns=columns)


@app.get("/get_statistics", name="Recommendation statistics")
async def get_statistics():
    """
    Возвращает статистику использования рекомендаций
    """
    return rec_store.stats()


# Команда запуска:
# uvicorn recommendations_service:app
