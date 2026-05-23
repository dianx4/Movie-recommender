"""
main.py — Punto de entrada del Recomendador de Películas Concurrente.

"""

import os
import signal
import threading
import time
import logging

import utils
import cache
import fetcher
import recommender
from sync import SHUTDOWN_EVENT, RESULT_QUEUE

from dotenv import load_dotenv
load_dotenv()

# ── Constantes configurables ─────────────────────────────────────────────────
NUM_CONSUMERS   = 3       # Hilos consumidores
MONITOR_INTERVAL = 5      # Segundos entre reportes del monitor
TOP_N           = 5       # Películas a mostrar en cada reporte
RUN_SECONDS     = 40      # Tiempo total de ejecución en demo

# IDs de películas a procesar (películas populares de TMDB)
MOVIE_IDS = [
    550, 13, 238, 278, 680, 299534, 19404,
    372058, 129, 424, 155, 389, 11, 603,
    27205, 157336, "tt0133093",  # <- ID string a propósito para probar robustez
]

# Filtra IDs inválidos
MOVIE_IDS = [m for m in MOVIE_IDS if isinstance(m, int)]

TMDB_API_KEY = utils.env("TMDB_API_KEY", "")   # Vacío → modo demo

logger = logging.getLogger(__name__)


# ── Hilo monitor ─────────────────────────────────────────────────────────────

def monitor_thread() -> None:
    """
    Cada MONITOR_INTERVAL segundos imprime las TOP_N películas mejor
    puntuadas de la caché y drena RESULT_QUEUE para no acumular entradas.
    """
    logger.info("Monitor iniciado (intervalo=%ds).", MONITOR_INTERVAL)
    while not SHUTDOWN_EVENT.is_set():
        SHUTDOWN_EVENT.wait(timeout=MONITOR_INTERVAL)

        # Drena resultados recientes (opcional: podrías procesarlos)
        recientes = []
        while not RESULT_QUEUE.empty():
            try:
                recientes.append(RESULT_QUEUE.get_nowait())
            except Exception:
                break

        top = cache.get_all_sorted()[:TOP_N]
        if not top:
            logger.info("Monitor: caché vacía todavía…")
            continue

        sep = "─" * 60
        print(f"\n{sep}")
        print(f"  TOP {TOP_N} RECOMENDACIONES  |  caché={cache.size()} películas")
        print(sep)
        for i, entry in enumerate(top, 1):
            print(f"  {i}. {utils.format_movie(entry)}")
        if recientes:
            titles = ", ".join(r["title"] for r in recientes)
            print(f"\n Recién procesadas: {titles}")
        print(sep)

    logger.info("Monitor finalizado.")


# ── Manejador de señal SIGINT (Ctrl-C) ───────────────────────────────────────

def _handle_sigint(signum, frame):
    logger.warning("Señal de interrupción recibida — iniciando apagado ordenado…")
    SHUTDOWN_EVENT.set()


# ── Función principal ─────────────────────────────────────────────────────────

def main() -> None:
    utils.setup_logging("INFO")
    signal.signal(signal.SIGINT, _handle_sigint)

    logger.info("=== Recomendador de Películas Concurrente ===")
    logger.info("Películas a procesar: %s", MOVIE_IDS)
    logger.info("API key TMDB: %s", "configurada" if TMDB_API_KEY else "ausente (modo demo)")

    # 1. Hilo productor
    prod = threading.Thread(
        target=fetcher.producer_thread,
        args=(MOVIE_IDS, False),
        name="Productor",
        daemon=True,
    )

    # 2. Hilos consumidores
    consumers = [
        threading.Thread(
            target=recommender.consumer_thread,
            args=(i, TMDB_API_KEY),
            name=f"Consumidor-{i}",
            daemon=True,
        )
        for i in range(1, NUM_CONSUMERS + 1)
    ]

    # 3. Hilo monitor
    mon = threading.Thread(
        target=monitor_thread,
        name="Monitor",
        daemon=True,
    )

    # ── Arranque ──────────────────────────────────────────────────────────────
    mon.start()
    for c in consumers:
        c.start()
    prod.start()

    logger.info("Todos los hilos iniciados.  Ejecución demo de %ds.", RUN_SECONDS)

    # Espera activa: se detiene antes si el productor termina
    deadline = time.time() + RUN_SECONDS
    while time.time() < deadline and not SHUTDOWN_EVENT.is_set():
        if not prod.is_alive() and all(not c.is_alive() for c in consumers):
            logger.info("Productor y consumidores han finalizado antes del timeout.")
            break
        time.sleep(0.5)

    # ── Apagado ordenado ──────────────────────────────────────────────────────
    SHUTDOWN_EVENT.set()
    logger.info("Esperando que terminen los hilos…")

    prod.join(timeout=5)
    for c in consumers:
        c.join(timeout=5)
    mon.join(timeout=MONITOR_INTERVAL + 2)

    # ── Reporte final ─────────────────────────────────────────────────────────
    top = cache.get_all_sorted()[:TOP_N]
    print("\n" + "═" * 60)
    print("  RECOMENDACIONES FINALES")
    print("═" * 60)
    for i, entry in enumerate(top, 1):
        print(f"  {i}. {utils.format_movie(entry)}")
    print("═" * 60)
    logger.info("Programa finalizado correctamente.")


if __name__ == "__main__":
    # Necesario para multiprocessing en Windows / macOS
    import multiprocessing
    multiprocessing.freeze_support()
    main()
