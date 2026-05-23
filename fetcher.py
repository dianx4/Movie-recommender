"""
fetcher.py — Descarga de metadata de películas desde la API de TMDB.

"""

import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

import cache
import utils
from sync import API_SEMAPHORE, MOVIE_QUEUE, SHUTDOWN_EVENT

logger = logging.getLogger(__name__)

TMDB_BASE = "https://api.themoviedb.org/3"
MAX_WORKERS = 8          # Hilos en el pool de I/O
POLL_INTERVAL = 0.05     # Segundos entre comprobaciones de la cola


# ── Datos simulados (modo demo sin API key real) ─────────────────────────────

_FAKE_MOVIES = [
    {"id": 550,   "title": "Fight Club",            "vote_average": 8.4, "release_date": "1999-10-15", "overview": "Un hombre insomne y un vendedor de jabón forman un club de lucha."},
    {"id": 13,    "title": "Forrest Gump",           "vote_average": 8.5, "release_date": "1994-07-06", "overview": "La vida extraordinaria de un hombre con un QI de 75."},
    {"id": 238,   "title": "The Godfather",          "vote_average": 8.7, "release_date": "1972-03-24", "overview": "El patriarca de una familia mafiosa transfiere el control a su hijo."},
    {"id": 278,   "title": "The Shawshank Redemption","vote_average": 8.7,"release_date": "1994-09-23", "overview": "Dos hombres se unen en la prisión de Shawshank."},
    {"id": 680,   "title": "Pulp Fiction",           "vote_average": 8.5, "release_date": "1994-10-14", "overview": "Las historias entrelazadas de criminales en Los Ángeles."},
    {"id": 299534,"title": "Avengers: Endgame",      "vote_average": 8.4, "release_date": "2019-04-26", "overview": "Los Vengadores se reúnen para revertir el chasquido de Thanos."},
    {"id": 19404, "title": "Dilwale Dulhania Le Jayenge","vote_average":8.7,"release_date":"1995-10-20","overview":"Simran se enamora de Raj en un viaje por Europa."},
    {"id": 372058,"title": "Your Name",              "vote_average": 8.5, "release_date": "2016-08-26", "overview": "Dos adolescentes descubren que se intercambian de cuerpos mientras duermen."},
    {"id": 129,   "title": "Spirited Away",          "vote_average": 8.5, "release_date": "2001-07-20", "overview": "Una niña entra en el mundo de los espíritus."},
    {"id": 424,   "title": "Schindler's List",       "vote_average": 8.6, "release_date": "1993-12-15", "overview": "Un empresario nazi salva a más de mil judíos del Holocausto."},
    {"id": 155,   "title": "The Dark Knight",        "vote_average": 8.5, "release_date": "2008-07-18", "overview": "Batman enfrenta al Joker en Gotham City."},
    {"id": 389,   "title": "12 Angry Men",           "vote_average": 8.5, "release_date": "1957-04-10", "overview": "Doce jurados deliberan sobre el destino de un joven acusado de asesinato."},
]

_FAKE_REVIEWS = [
    "Una obra maestra del cine moderno.",
    "Actuaciones increíbles y guión brillante.",
    "No me esperaba este giro al final.",
    "La fotografía es impresionante.",
    "Un clásico que nunca pasa de moda.",
    "Entretenida pero predecible.",
    "El ritmo es un poco lento al principio.",
    "De las mejores películas que he visto.",
]


def _fake_fetch(movie_id: int) -> dict:
    """Simula una llamada a la API con latencia aleatoria."""
    time.sleep(random.uniform(0.1, 0.6))   # Simula red
    for m in _FAKE_MOVIES:
        if m["id"] == movie_id:
            return dict(m)
    # ID desconocido → genera datos ficticios
    return {
        "id": movie_id,
        "title": f"Película #{movie_id}",
        "vote_average": round(random.uniform(5.0, 9.0), 1),
        "release_date": f"{random.randint(1970, 2024)}-01-01",
        "overview": "Una historia épica de proporciones cinematográficas.",
    }


def _fake_reviews(movie_id: int) -> list[str]:
    """Devuelve 2–4 reseñas simuladas."""
    time.sleep(random.uniform(0.05, 0.3))
    k = random.randint(2, 4)
    return random.sample(_FAKE_REVIEWS, k)


# ── Funciones reales (requieren TMDB_API_KEY) ────────────────────────────────

@utils.retry(max_attempts=3, base_delay=1.0, exceptions=(Exception,))
def _real_fetch_details(movie_id: int, api_key: str) -> dict:
    with API_SEMAPHORE:                     # ← Semáforo adquirido
        url = f"{TMDB_BASE}/movie/{movie_id}"
        resp = requests.get(url, params={"api_key": api_key}, timeout=10)
        resp.raise_for_status()
        return resp.json()


@utils.retry(max_attempts=3, base_delay=1.0, exceptions=(Exception,))
def _real_fetch_reviews(movie_id: int, api_key: str) -> list[str]:
    with API_SEMAPHORE:                     # ← Semáforo adquirido
        url = f"{TMDB_BASE}/movie/{movie_id}/reviews"
        resp = requests.get(url, params={"api_key": api_key, "page": 1}, timeout=10)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        return [r["content"][:200] for r in results[:5]]


# ── Función pública: descarga detalles + reseñas de una película ─────────────

def fetch_movie(movie_id: int, api_key: str = "") -> Optional[dict]:
    """
    Descarga metadata y reseñas de *movie_id*.

    Si no hay api_key usa datos simulados.
    Protegido por API_SEMAPHORE para no saturar la API.
    """
    logger.info("Descargando id=%s …", movie_id)
    cached = cache.get(movie_id)
    if cached:
        logger.info("id=%s encontrado en caché, saltando descarga.", movie_id)
        return cached

    try:
        if api_key and REQUESTS_AVAILABLE:
            details = _real_fetch_details(movie_id, api_key)
            reviews = _real_fetch_reviews(movie_id, api_key)
        else:
            details = _fake_fetch(movie_id)
            reviews = _fake_reviews(movie_id)

        details["reviews"] = reviews
        logger.info("id=%s  '%s' descargado con %d reseña(s).",
                    movie_id, details.get("title", "?"), len(reviews))
        return details

    except Exception as exc:
        logger.error("Error descargando id=%s: %s", movie_id, exc)
        return None


# ── Pool de I/O: lanza múltiples fetches en paralelo ────────────────────────

def fetch_batch(movie_ids: list[int], api_key: str = "") -> list[dict]:
    """
    Usa ThreadPoolExecutor para descargar una lista de películas en paralelo.
    Devuelve la lista de resultados (sin None).
    """
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="fetcher") as pool:
        futures = {pool.submit(fetch_movie, mid, api_key): mid for mid in movie_ids}
        for future in as_completed(futures):
            mid = futures[future]
            try:
                data = future.result()
                if data:
                    results.append(data)
            except Exception as exc:
                logger.error("Future id=%s lanzó: %s", mid, exc)
    return results


# ── Hilo productor: alimenta MOVIE_QUEUE con IDs ─────────────────────────────

def producer_thread(movie_ids: list[int], repeat: bool = False) -> None:
    """
    Hilo productor.  Encola los IDs de *movie_ids* en MOVIE_QUEUE.
    Si *repeat=True* cicla indefinidamente hasta que SHUTDOWN_EVENT se active.
    Marca el fin de la cola poniendo None (señal de veneno) por cada consumidor.
    """
    logger.info("Productor iniciado con %d películas.", len(movie_ids))
    iteration = 0
    while not SHUTDOWN_EVENT.is_set():
        iteration += 1
        logger.info("Productor — ciclo %d", iteration)
        for mid in movie_ids:
            if SHUTDOWN_EVENT.is_set():
                break
            MOVIE_QUEUE.put(mid)
            logger.debug("Productor encoló id=%s", mid)
        if not repeat:
            break
        # Pausa entre ciclos para no saturar la API
        SHUTDOWN_EVENT.wait(timeout=30)

    # Señal de veneno: un None por consumidor (main.py sabe cuántos hay)
    from main import NUM_CONSUMERS          # import tardío para evitar ciclos
    for _ in range(NUM_CONSUMERS):
        MOVIE_QUEUE.put(None)
    logger.info("Productor finalizado.")
