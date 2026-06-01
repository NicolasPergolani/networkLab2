import asyncio
import statistics
import time

import aiohttp

# URL base objetivo de la API y constantes de configuracion de prueba.
BASE_URL = "http://127.0.0.1:8080"
TOTAL_INGEST_REQUESTS = 500
HEALTH_PROBE_INTERVAL_SECONDS = 0.05
STATS_TIMEOUT_SECONDS = 90


# Envia una request de ingest y devuelve si fue aceptada por la API.
async def send_ingest_request(session: aiohttp.ClientSession, event_id: int) -> bool:
    url = f"{BASE_URL}/ingest?id={event_id}"

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as response:
            if response.status not in (200, 202):
                return False

            payload = await response.json()
            return bool(payload.get("accepted"))
    except Exception:
        return False


# Sondea /health continuamente mientras hay trafico de ingest, guardando latencias.
async def health_probe_loop(
    session: aiohttp.ClientSession,
    stop_event: asyncio.Event,
    latencies_ms: list[float],
) -> None:
    while not stop_event.is_set():
        started_at = time.perf_counter()

        try:
            async with session.get(
                f"{BASE_URL}/health", timeout=aiohttp.ClientTimeout(total=2)
            ) as response:
                await response.read()
                if response.status == 200:
                    latency_ms = (time.perf_counter() - started_at) * 1000
                    latencies_ms.append(latency_ms)
        except Exception:
            pass

        await asyncio.sleep(HEALTH_PROBE_INTERVAL_SECONDS)


    # Hace polling de /stats hasta llegar al valor esperado o expirar el timeout.
async def wait_for_expected_stats(
    session: aiohttp.ClientSession, expected_count: int
) -> int:
    started_at = time.perf_counter()
    latest_count = 0

    while (time.perf_counter() - started_at) < STATS_TIMEOUT_SECONDS:
        try:
            async with session.get(
                f"{BASE_URL}/stats", timeout=aiohttp.ClientTimeout(total=3)
            ) as response:
                if response.status == 200:
                    payload = await response.json()
                    latest_count = int(payload.get("processedEvents", 0))
                    if latest_count >= expected_count:
                        return latest_count
        except Exception:
            pass

        await asyncio.sleep(0.2)

    return latest_count


# Lee el valor actual de /stats para validar por delta.
async def fetch_current_stats(session: aiohttp.ClientSession) -> int:
    try:
        async with session.get(
            f"{BASE_URL}/stats", timeout=aiohttp.ClientTimeout(total=3)
        ) as response:
            if response.status != 200:
                return 0

            payload = await response.json()
            return int(payload.get("processedEvents", 0))
    except Exception:
        return 0


# Calcula percentil usando seleccion simple por indice sobre muestras ordenadas.
def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0

    ordered = sorted(values)
    index = int((len(ordered) - 1) * p)
    return ordered[index]


# Escenario principal: rafaga de ingest + probes concurrentes de health + validacion final.
async def main() -> None:
    # Limite alto de conexiones para evitar cuello de botella del cliente en la rafaga.
    connector = aiohttp.TCPConnector(limit=1000)

    async with aiohttp.ClientSession(connector=connector) as session:
        baseline_count = await fetch_current_stats(session)
        latencies_ms: list[float] = []
        stop_event = asyncio.Event()

        # Corre en paralelo mientras se envian requests de ingest.
        health_task = asyncio.create_task(health_probe_loop(session, stop_event, latencies_ms))

        # Crea exactamente 500 requests de ingest concurrentes.
        ingest_tasks = [
            asyncio.create_task(send_ingest_request(session, event_id))
            for event_id in range(1, TOTAL_INGEST_REQUESTS + 1)
        ]

        ingest_results = await asyncio.gather(*ingest_tasks)

        accepted_requests = sum(1 for item in ingest_results if item)

        expected_final_count = baseline_count + accepted_requests
        final_count = await wait_for_expected_stats(session, expected_final_count)
        processed_delta = final_count - baseline_count

        # Detiene los probes en segundo plano solo al finalizar la verificacion.
        stop_event.set()
        await health_task

        print("=== Stress Test Result ===")
        print(f"Ingest requests sent: {TOTAL_INGEST_REQUESTS}")
        print(f"Accepted by API: {accepted_requests}")
        print(f"Initial processedEvents in /stats: {baseline_count}")
        print(f"Final processedEvents in /stats: {final_count}")
        print(f"Processed delta during test: {processed_delta}")

        if latencies_ms:
            print("--- /health latency (ms) ---")
            print(f"Samples: {len(latencies_ms)}")
            print(f"Min: {min(latencies_ms):.2f}")
            print(f"Avg: {statistics.mean(latencies_ms):.2f}")
            print(f"P50: {percentile(latencies_ms, 0.50):.2f}")
            print(f"P95: {percentile(latencies_ms, 0.95):.2f}")
            print(f"Max: {max(latencies_ms):.2f}")
        else:
            print("No /health samples collected.")

        if processed_delta == accepted_requests:
            print("Validation: OK -> Counter matches accepted ingest events.")
        else:
            print("Validation: WARNING -> Counter mismatch detected.")


if __name__ == "__main__":
    asyncio.run(main())
