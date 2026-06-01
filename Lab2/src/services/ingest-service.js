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
