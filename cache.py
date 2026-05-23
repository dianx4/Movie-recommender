"""
cache.py — Caché local protegida contra escrituras concurrentes.

"""

import time
import logging
from typing import Any, Optional

from sync import CACHE_LOCK

logger = logging.getLogger(__name__)

# Almacén en memoria: {movie_id: {"data": ..., "ts": timestamp, "score": ...}}
_store: dict = {}

# Tiempo de vida de cada entrada (segundos).  Pasado este tiempo se considera
# "stale" y el fetcher la refresca en el siguiente ciclo.
TTL_SECONDS = 300  # 5 minutos


def get(movie_id: int) -> Optional[dict]:
    """Devuelve la entrada si existe y no ha expirado; None en caso contrario."""
    entry = _store.get(movie_id)
    if entry is None:
        return None
    age = time.time() - entry["ts"]
    if age > TTL_SECONDS:
        logger.debug("Cache MISS (expirado) id=%s age=%.0fs", movie_id, age)
        return None
    logger.debug("Cache HIT id=%s age=%.0fs", movie_id, age)
    return entry


def set(movie_id: int, data: dict, score: float = 0.0) -> None:
    """Escribe/actualiza una entrada de forma thread-safe."""
    with CACHE_LOCK:                        # ← Lock adquirido
        _store[movie_id] = {
            "data": data,
            "score": score,
            "ts": time.time(),
        }
    logger.debug("Cache SET id=%s score=%.2f", movie_id, score)


def get_all_sorted() -> list[dict]:
    """Devuelve todas las entradas vigentes ordenadas por score descendente."""
    now = time.time()
    with CACHE_LOCK:
        valid = [
            v for v in _store.values()
            if now - v["ts"] <= TTL_SECONDS
        ]
    return sorted(valid, key=lambda x: x["score"], reverse=True)


def size() -> int:
    """Número de entradas actualmente en caché (incluyendo expiradas)."""
    return len(_store)
