"""
utils.py — Funciones de utilidad reutilizables en todo el proyecto.

"""

import logging
import os
import sys
import time
from functools import wraps
from typing import Callable, Any


# ── Configuración del logger del proyecto ───────────────────────────────────

def setup_logging(level: str = "INFO") -> None:
    """Configura el formato de log con timestamp e hilo de origen."""
    fmt = (
        "%(asctime)s  [%(threadName)-20s]  %(levelname)-8s  "
        "%(name)s: %(message)s"
    )
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )


# ── Decorador de reintentos con back-off exponencial ────────────────────────

def retry(max_attempts: int = 3, base_delay: float = 1.0, exceptions=(Exception,)):
    """
    Decora una función para que se reintente automáticamente si lanza
    alguna de las excepciones indicadas, con espera exponencial entre intentos.

    Ejemplo:
        @retry(max_attempts=3, base_delay=0.5)
        def fetch_data(): ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    delay = base_delay * (2 ** (attempt - 1))
                    logging.getLogger(__name__).warning(
                        "Intento %d/%d fallido en '%s': %s — reintentando en %.1fs",
                        attempt, max_attempts, func.__name__, exc, delay,
                    )
                    time.sleep(delay)
            raise last_exc  # type: ignore[misc]
        return wrapper
    return decorator


# ── Lectura segura de variables de entorno ──────────────────────────────────

def env(key: str, default: str = "") -> str:
    """Lee una variable de entorno; devuelve *default* si no existe."""
    return os.environ.get(key, default)


# ── Formateo de resultados para consola ─────────────────────────────────────

def format_movie(entry: dict) -> str:
    """Devuelve una representación legible de una entrada de caché."""
    data  = entry.get("data", {})
    score = entry.get("score", 0.0)
    title = data.get("title", "Sin título")
    year  = str(data.get("release_date", "????"))[:4]
    rating = data.get("vote_average", 0.0)
    overview = (data.get("overview", "") or "")[:80]
    return (
        f"   {title} ({year})   {rating:.1f}   score={score:.3f}\n"
        f"       {overview}…"
    )


# ── Temporización simple ─────────────────────────────────────────────────────

class Timer:
    """Context manager para medir tiempo de ejecución de un bloque."""

    def __init__(self, label: str = ""):
        self.label = label
        self.elapsed = 0.0

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_):
        self.elapsed = time.perf_counter() - self._start
        if self.label:
            logging.getLogger(__name__).debug(
                "%s tomó %.3fs", self.label, self.elapsed
            )
