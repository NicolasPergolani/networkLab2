const { INGEST_CRASH_PROBABILITY } = require('../shared/constants');

// Parsea y valida el parametro query id de ingest.
function parseNumericId(value) {
  if (typeof value !== 'string') {
    return null;
  }

  const normalized = value.trim();
  if (!/^[-]?\d+$/.test(normalized)) {
    return null;
  }

  const parsed = Number.parseInt(normalized, 10);
  if (!Number.isInteger(parsed)) {
    return null;
  }

  return parsed;
}

// Valida el payload de la request y delega ejecucion a la cola del thread pool.
function acceptIngest(idParam, threadPool) {
  const parsedId = parseNumericId(idParam);
  if (parsedId === null) {
    return {
      ok: false,
      error: 'invalid_or_missing_id',
      statusCode: 400,
    };
  }

  // Fallo simulado configurable para probar self-healing sin bloquear el stress test.
  // Se aplica antes de encolar para evitar marcar como "aceptadas" tareas que se perderian.
  const crashProbability = Number.isFinite(INGEST_CRASH_PROBABILITY)
    ? Math.min(1, Math.max(0, INGEST_CRASH_PROBABILITY))
    : 0;
  if (Math.random() < crashProbability) {
    setImmediate(() => {
      process.stderr.write(
        `[CRASH SIMULADO] Worker PID=${process.pid} muere antes de encolar id=${parsedId}\n`
      );
      process.exit(1);
    });

    return {
      ok: false,
      error: 'simulated_worker_crash',
      statusCode: 503,
    };
  }

  const enqueueResult = threadPool.enqueue(parsedId);
  if (!enqueueResult.ok) {
    // Camino de backpressure: la cola esta llena, este worker rechaza nuevas tareas.
    return {
      ok: false,
      error: 'ingest_queue_full',
      statusCode: 503,
      queueSize: enqueueResult.queueSize,
      maxQueueSize: enqueueResult.maxQueueSize,
    };
  }

  return {
    ok: true,
    id: parsedId,
    statusCode: 202,
    queueSize: enqueueResult.queueSize,
    maxQueueSize: enqueueResult.maxQueueSize,
  };
}

module.exports = {
  acceptIngest,
};
