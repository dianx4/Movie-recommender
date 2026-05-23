"""
sync.py — Primitivas de sincronización compartidas para todo el proyecto.

"""

import threading
import queue

# ── Semáforo: limita las llamadas simultáneas a la API externa ──────────────
# Máximo 5 peticiones en vuelo al mismo tiempo (rate-limit cortés con TMDB).
API_SEMAPHORE = threading.Semaphore(5)

# ── Lock: protege escrituras en la caché compartida ─────────────────────────
# Un solo hilo puede escribir a la vez; lecturas simultáneas son seguras
# porque dict.__getitem__ es atómico en CPython (GIL), pero las escrituras
# compuestas (check-then-set) requieren el lock explícito.
CACHE_LOCK = threading.Lock()

# ── Evento: señal de "apagado" para que los consumidores paren limpiamente ──
SHUTDOWN_EVENT = threading.Event()

# ── Cola principal de trabajo (productor → consumidores) ────────────────────
# maxsize=0 → ilimitada; ajusta si quieres back-pressure.
MOVIE_QUEUE: queue.Queue = queue.Queue(maxsize=0)

# ── Cola de resultados procesados (consumidores → hilo de presentación) ─────
RESULT_QUEUE: queue.Queue = queue.Queue(maxsize=0)
