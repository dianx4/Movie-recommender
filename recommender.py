"""
recommender.py — Cálculo de puntuaciones de recomendación.

"""

import logging
import math
import multiprocessing
import random
import time
from typing import Optional

import cache
from sync import MOVIE_QUEUE, RESULT_QUEUE, SHUTDOWN_EVENT
import fetcher

logger = logging.getLogger(__name__)

MIN_MP_BATCH = 4          # Mínimo de películas para activar multiprocessing
GENRE_WEIGHTS = {         # Géneros preferidos (simulan perfil de usuario)
    28: 1.2,   # Acción
    35: 1.1,   # Comedia
    18: 1.3,   # Drama
    878: 1.25, # Ciencia ficción
    27: 0.9,   # Terror
    10749: 1.0,# Romance
}


# ── Función pura de scoring (apta para multiprocessing) ─────────────────────

def _score_single(data: dict) -> tuple[int, float]:
    """
    Calcula el score de recomendación de una película.

    Fórmula ponderada:
      score = rating_norm * 0.40
            + popularity_norm * 0.20
            + recency_norm * 0.15
            + sentiment_norm * 0.15
            + genre_boost * 0.10

    Devuelve (movie_id, score).
    """
    movie_id = data.get("id", 0)

    # --- Rating normalizado (0–10 → 0–1) ---
    rating = float(data.get("vote_average", 0))
    rating_norm = min(rating / 10.0, 1.0)

    # --- Popularidad normalizada (log scale) ---
    popularity = float(data.get("popularity", 1.0))
    popularity_norm = min(math.log1p(popularity) / 10.0, 1.0)

    # --- Recencia (películas más recientes puntúan más) ---
    try:
        year = int(str(data.get("release_date", "2000"))[:4])
    except ValueError:
        year = 2000
    recency_norm = max(0.0, (year - 1970) / (2025 - 1970))

    # --- Sentimiento de reseñas (simulado: longitud promedio como proxy) ---
    reviews = data.get("reviews", [])
    if reviews:
        avg_len = sum(len(r) for r in reviews) / len(reviews)
        sentiment_norm = min(avg_len / 500.0, 1.0)
    else:
        sentiment_norm = 0.5

    # --- Boost por género preferido ---
    genres = data.get("genre_ids", [])
    if genres:
        boosts = [GENRE_WEIGHTS.get(g, 1.0) for g in genres]
        genre_boost = sum(boosts) / len(boosts)
    else:
        genre_boost = 1.0

    score = (
        rating_norm     * 0.40 +
        popularity_norm * 0.20 +
        recency_norm    * 0.15 +
        sentiment_norm  * 0.15 +
        (genre_boost - 1.0) * 0.10   # Normalizado alrededor de 0
    )
    # Pequeño ruido para que los scores no sean idénticos en demo
    score += random.uniform(-0.01, 0.01)
    return movie_id, round(max(0.0, min(score, 1.0)), 4)


# ── Cómputo en lote (opcionalmente con multiprocessing) ─────────────────────

def compute_scores(movies: list[dict]) -> list[tuple[int, float]]:
    """
    Calcula scores para una lista de películas.

    Usa multiprocessing.Pool si el lote es suficientemente grande para que
    valga la pena el overhead de crear procesos (CPU-bound).
    """
    if not movies:
        return []

    if len(movies) >= MIN_MP_BATCH:
        logger.info("Usando multiprocessing.Pool con %d películas.", len(movies))
        try:
            with multiprocessing.Pool(processes=min(4, len(movies))) as pool:
                results = pool.map(_score_single, movies)
            return results
        except Exception as exc:
            logger.warning("multiprocessing falló (%s), cayendo a modo secuencial.", exc)

    # Fallback secuencial
    return [_score_single(m) for m in movies]


# ── Hilo consumidor ──────────────────────────────────────────────────────────

def consumer_thread(consumer_id: int, api_key: str = "") -> None:
    """
    Hilo consumidor.  Saca IDs de MOVIE_QUEUE, descarga metadata,
    calcula score y actualiza la caché.

    Termina cuando recibe None (señal de veneno del productor) o cuando
    SHUTDOWN_EVENT está activo y la cola está vacía.
    """
    logger.info("Consumidor #%d iniciado.", consumer_id)
    while True:
        try:
            movie_id = MOVIE_QUEUE.get(timeout=2)
        except Exception:
            # Cola vacía y timeout alcanzado
            if SHUTDOWN_EVENT.is_set():
                break
            continue

        if movie_id is None:              # Señal de veneno → terminar
            MOVIE_QUEUE.task_done()
            logger.info("Consumidor #%d recibió señal de parada.", consumer_id)
            break

        logger.info("Consumidor #%d procesando id=%s", consumer_id, movie_id)

        # 1. Descarga (usa caché si ya existe)
        data = fetcher.fetch_movie(movie_id, api_key)
        if data is None:
            MOVIE_QUEUE.task_done()
            continue

        # 2. Calcula score
        _, score = _score_single(data)

        # 3. Guarda en caché (CACHE_LOCK interno en cache.set)
        cache.set(movie_id, data, score)

        # 4. Publica resultado
        RESULT_QUEUE.put({"movie_id": movie_id, "score": score,
                          "title": data.get("title", "?")})

        MOVIE_QUEUE.task_done()
        logger.info("Consumidor #%d — id=%s score=%.4f guardado.",
                    consumer_id, movie_id, score)

    logger.info("Consumidor #%d finalizado.", consumer_id)
