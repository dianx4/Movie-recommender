# 🎬 CineBot — Recomendador de Películas en Tiempo Real

Sistema concurrente en Python que consulta la API de TMDB,
calcula recomendaciones personalizadas y las actualiza en tiempo real.

## ¿Qué hace?
- Descarga metadata y reseñas de películas en paralelo
- Calcula un score de recomendación por cada película
- Muestra un ranking TOP-5 actualizado en tiempo real
- Usa caché local con tiempo de vida (TTL)

## Concurrencia implementada
- `threading.Thread` — hilos Productor, Consumidores y Monitor
- `ThreadPoolExecutor` — hasta 8 peticiones HTTP en paralelo
- `threading.Semaphore` — límite de 5 llamadas simultáneas a la API
- `threading.Lock` — escrituras seguras en la caché
- `threading.Event` — apagado ordenado
- `queue.Queue` — patrón productor–consumidor
- `multiprocessing.Pool` — cómputo de scores en paralelo

## Instalación
pip install requests python-dotenv

## Configuración
1. Copia `.env.example` y renómbralo `.env`
2. Agrega tu API key de TMDB:
TMDB_API_KEY=tu_key_aqui

## Uso
python main.py

## Estructura
- `main.py` — punto de entrada
- `fetcher.py` — descarga de API
- `recommender.py` — cálculo de scores
- `cache.py` — caché local
- `sync.py` — primitivas de sincronización
- `utils.py` — utilidades compartidas
