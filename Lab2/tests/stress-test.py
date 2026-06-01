import asyncio
import statistics
import time

import aiohttp

# URL base objetivo de la API y constantes de configuracion de prueba.
BASE_URL = "http://127.0.0.1:8080"
TOTAL_INGEST_REQUESTS = 500
HEALTH_PROBE_INTERVAL_SECONDS = 0.05
STATS_TIMEOUT_SECONDS = 90
HEALTH_P95_TARGET_MS = 10


# Imprime mensajes de estado en tiempo real para facilitar la demo en consola.
def log_progress(message: str) -> None:
    print(message, flush=True)


# Convierte processedByWorker en un dict estable {pid: contador} con enteros.
def normalize_processed_by_worker(payload: dict) -> dict[str, int]:
    raw = payload.get("processedByWorker")
    if not isinstance(raw, dict):
        return {}

    normalized: dict[str, int] = {}
    for pid, count in raw.items():
        normalized[str(pid)] = int(count)

    return normalized


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
    failures: list[int],
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
                else:
                    failures[0] += 1
        except Exception:
            failures[0] += 1

        await asyncio.sleep(HEALTH_PROBE_INTERVAL_SECONDS)


    # Hace polling de /stats hasta llegar al valor esperado o expirar el timeout.
async def wait_for_expected_stats(
    session: aiohttp.ClientSession, expected_count: int
) -> dict:
    started_at = time.perf_counter()
    latest_payload: dict = {"processedEvents": 0, "processedByWorker": {}}
    last_reported_at = 0.0
    last_value = -1
    last_change_at = started_at

    while (time.perf_counter() - started_at) < STATS_TIMEOUT_SECONDS:
        try:
            async with session.get(
                f"{BASE_URL}/stats", timeout=aiohttp.ClientTimeout(total=3)
            ) as response:
                if response.status == 200:
                    payload = await response.json()
                    latest_payload = {
                        "processedEvents": int(payload.get("processedEvents", 0)),
                        "processedByWorker": normalize_processed_by_worker(payload),
                    }

                    elapsed = time.perf_counter() - started_at
                    current_value = latest_payload["processedEvents"]
                    if current_value != last_value:
                        last_value = current_value
                        last_change_at = time.perf_counter()

                    if elapsed - last_reported_at >= 2:
                        log_progress(
                            "[progress] /stats processedEvents="
                            f"{latest_payload['processedEvents']}"
                            f"/{expected_count} (t={elapsed:.1f}s)"
                        )
                        last_reported_at = elapsed

                    if latest_payload["processedEvents"] >= expected_count:
                        return latest_payload

                    # Corta temprano si no hay avance por mucho tiempo para evitar espera innecesaria.
                    stalled_for = time.perf_counter() - last_change_at
                    if stalled_for >= 12:
                        log_progress(
                            "[warning] /stats sin avance por "
                            f"{stalled_for:.1f}s; continuo con el ultimo valor observado."
                        )
                        return latest_payload
        except Exception:
            pass

        await asyncio.sleep(0.2)

    return latest_payload


# Lee el valor actual de /stats para validar por delta y distribucion por worker.
async def fetch_current_stats(session: aiohttp.ClientSession) -> dict:
    try:
        async with session.get(
            f"{BASE_URL}/stats", timeout=aiohttp.ClientTimeout(total=3)
        ) as response:
            if response.status != 200:
                return {"processedEvents": 0, "processedByWorker": {}}

            payload = await response.json()
            return {
                "processedEvents": int(payload.get("processedEvents", 0)),
                "processedByWorker": normalize_processed_by_worker(payload),
            }
    except Exception:
        return {"processedEvents": 0, "processedByWorker": {}}


def compute_per_worker_delta(
    baseline_by_worker: dict[str, int], final_by_worker: dict[str, int]
) -> dict[str, int]:
    # Une ambos conjuntos de PIDs para calcular incrementos aun si aparecen nuevos workers.
    pids = set(baseline_by_worker.keys()) | set(final_by_worker.keys())
    delta: dict[str, int] = {}

    for pid in pids:
        delta[pid] = final_by_worker.get(pid, 0) - baseline_by_worker.get(pid, 0)

    # Filtra solo workers con trabajo positivo durante esta corrida.
    return {pid: count for pid, count in delta.items() if count > 0}


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
    connector = aiohttp.TCPConnector(limit=1000, force_close=True)

    async with aiohttp.ClientSession(connector=connector) as session:
        log_progress("[start] Ejecutando stress test...")
        # Baseline inicial para medir solo lo que procesa esta prueba.
        baseline_stats = await fetch_current_stats(session)
        baseline_count = int(baseline_stats.get("processedEvents", 0))
        baseline_by_worker = normalize_processed_by_worker(baseline_stats)
        latencies_ms: list[float] = []
        health_failures = [0]
        stop_event = asyncio.Event()

        # Corre en paralelo mientras se envian requests de ingest.
        health_task = asyncio.create_task(
            health_probe_loop(session, stop_event, latencies_ms, health_failures)
        )

        # Rafaga inicial requerida por consigna: 500 requests simultaneas.
        ingest_tasks = [
            asyncio.create_task(send_ingest_request(session, event_id))
            for event_id in range(1, TOTAL_INGEST_REQUESTS + 1)
        ]

        ingest_results = await asyncio.gather(*ingest_tasks)
        accepted_requests = sum(1 for item in ingest_results if item)

        log_progress("[progress] Envio de ingest finalizado, validando contador...")

        expected_final_count = baseline_count + TOTAL_INGEST_REQUESTS
        # Espera hasta que /stats refleje el objetivo completo de 500 eventos.
        final_stats = await wait_for_expected_stats(session, expected_final_count)
        final_count = int(final_stats.get("processedEvents", 0))
        final_by_worker = normalize_processed_by_worker(final_stats)
        processed_delta = final_count - baseline_count
        # Distribucion efectiva por worker para esta corrida puntual.
        per_worker_delta = compute_per_worker_delta(baseline_by_worker, final_by_worker)

        # Detiene los probes en segundo plano solo al finalizar la verificacion.
        stop_event.set()
        await health_task

        # ── helpers ───────────────────────────────────────────
        SEP = "=" * 52
        ok  = lambda v: "OK   " if v else "FALLO"

        c1_ok = accepted_requests == TOTAL_INGEST_REQUESTS

        # ── encabezado ────────────────────────────────────────
        print()
        print(SEP)
        print("           STRESS TEST — RESULTADOS")
        print(SEP)

        # ── Criterio 1 ────────────────────────────────────────
        print(f"\n[C1] Rafaga /ingest")
        print(f"  Enviadas  : {TOTAL_INGEST_REQUESTS}")
        print(f"  Aceptadas : {accepted_requests}")
        if accepted_requests == 0:
            print(f"  [{ok(False)}] Node no esta corriendo o 0 aceptadas.")
        elif accepted_requests == TOTAL_INGEST_REQUESTS:
            print(f"  [{ok(True)}] Entraron exactamente 500 peticiones.")
        else:
            print(f"  [{ok(False)}] Entraron {accepted_requests}/500 peticiones.")

        # ── Criterio 2 ────────────────────────────────────────
        print(f"\n[C2] /health bajo carga")
        if latencies_ms:
            p95_ms = percentile(latencies_ms, 0.95)
            avg_ms = statistics.mean(latencies_ms)
            c2_ok  = health_failures[0] == 0 and p95_ms <= HEALTH_P95_TARGET_MS
            print(f"  Muestras  : {len(latencies_ms)}   Fallos: {health_failures[0]}")
            print(
                f"  Promedio  : {avg_ms:.2f} ms   P95: {p95_ms:.2f} ms "
                f"(objetivo <= {HEALTH_P95_TARGET_MS} ms)"
            )
            if c2_ok:
                print(
                    f"  [{ok(True)}] Respondio sin bloqueos con latencia cercana a 5 ms "
                    f"(P95={p95_ms:.2f} ms)."
                )
            else:
                print(
                    f"  [{ok(False)}] {health_failures[0]} fallos o "
                    f"P95={p95_ms:.2f} ms > {HEALTH_P95_TARGET_MS} ms."
                )
        else:
            c2_ok = False
            print(f"  [{ok(False)}] Sin muestras.")

        # ── Criterio 3 ────────────────────────────────────────
        c3_ok = (
            accepted_requests == TOTAL_INGEST_REQUESTS
            and processed_delta == TOTAL_INGEST_REQUESTS
        )
        print(f"\n[C3] Contador compartido (Atomics)")
        print(
            f"  Objetivo  : {TOTAL_INGEST_REQUESTS}   "
            f"Aceptadas: {accepted_requests}   Procesadas: {processed_delta}   "
            f"Drift: {abs(processed_delta - TOTAL_INGEST_REQUESTS)}"
        )
        if accepted_requests == 0:
            print(f"  [{ok(False)}] Sin ingestas, no se puede validar.")
        elif c3_ok:
            print(
                f"  [{ok(True)}] delta={processed_delta} == objetivo={TOTAL_INGEST_REQUESTS}, drift=0."
            )
        else:
            print(
                f"  [{ok(False)}] objetivo={TOTAL_INGEST_REQUESTS}, "
                f"aceptadas={accepted_requests}, procesadas={processed_delta}."
            )

        # ── Workers ───────────────────────────────────────────
        if per_worker_delta:
            print(f"\n[WORKERS] Distribucion por proceso")
            total_w = sum(per_worker_delta.values())
            for pid, count in sorted(per_worker_delta.items(), key=lambda x: -x[1]):
                pct = count / total_w * 100 if total_w else 0
                print(f"  PID {pid}: {count:>4} eventos ({pct:.1f}%)")

        # ── Veredicto ─────────────────────────────────────────
        all_ok = c1_ok and c2_ok and c3_ok
        print(f"\n  C1={ok(c1_ok)}  C2={ok(c2_ok)}  C3={ok(c3_ok)}")
        print("  TODOS LOS CRITERIOS CUMPLIDOS" if all_ok else "  HAY CRITERIOS SIN CUMPLIR")
        print(SEP)

        # Devuelve codigo de salida distinto de cero cuando no se cumple la consigna.
        if not all_ok:
            raise SystemExit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_progress("\n[interrupted] Prueba cancelada por el usuario (Ctrl+C).")
