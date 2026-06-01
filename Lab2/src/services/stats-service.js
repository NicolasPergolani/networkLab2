const { MESSAGE_TYPES } = require('../shared/constants');

// Map<requestId, { resolve, timeout }> para correlacionar requests /stats en vuelo.
const pendingStatsRequests = new Map();
let listenerAttached = false;

// Adjunta un unico listener a nivel de proceso para evitar uno por request.
function attachStatsResponseListener() {
  if (listenerAttached) {
    return;
  }

  process.on('message', (message) => {
    if (!message || typeof message !== 'object') {
      return;
    }

    if (message.type !== MESSAGE_TYPES.STATS_RESPONSE) {
      return;
    }

    const pending = pendingStatsRequests.get(message.requestId);
    if (!pending) {
      return;
    }

    pendingStatsRequests.delete(message.requestId);
    clearTimeout(pending.timeout);
    pending.resolve({
      processedEvents: Number(message.processedEvents) || 0,
      processedByWorker: message.processedByWorker,
    });
  });

  listenerAttached = true;
}

function buildRequestId() {
  // Suficiente unicidad para correlacion de requests dentro del proceso.
  return `${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

// Normaliza el mapa de conteo por worker para devolver siempre numeros validos.
function normalizeProcessedByWorker(value) {
  if (!value || typeof value !== 'object') {
    return {};
  }

  const normalized = {};

  for (const [workerPid, count] of Object.entries(value)) {
    normalized[String(workerPid)] = Number(count) || 0;
  }

  return normalized;
}

// Solicita el valor global del contador al master via IPC del cluster.
function getStats(timeoutMs = 2000) {
  if (typeof process.send !== 'function') {
    throw new Error('stats endpoint requires cluster IPC');
  }

  attachStatsResponseListener();

  const requestId = buildRequestId();

  return new Promise((resolve, reject) => {
    // Proteccion ante requests colgadas si el master no responde a tiempo.
    const timeout = setTimeout(() => {
      pendingStatsRequests.delete(requestId);
      reject(new Error('stats_request_timeout'));
    }, timeoutMs);

    pendingStatsRequests.set(requestId, {
      resolve: (payload) => {
        resolve({
          processedEvents: Number(payload?.processedEvents) || 0,
          processedByWorker: normalizeProcessedByWorker(payload?.processedByWorker),
        });
      },
      timeout,
    });

    process.send({ type: MESSAGE_TYPES.STATS_REQUEST, requestId });
  });
}

module.exports = {
  getStats,
};
