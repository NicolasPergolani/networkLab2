// Valores globales de configuracion en runtime, sobreescribibles por variables de entorno.
const PORT = Number.parseInt(process.env.PORT || '8080', 10);
// 4 bytes == un slot Int32 para el contador global de eventos procesados.
const SHARED_COUNTER_BYTES = 4;
// Requisito del TP: usar la mitad de los nucleos de CPU disponibles.
const WORKER_FRACTION = 0.5;
// Guarda de backpressure para la cola de ingest por worker.
const INGEST_QUEUE_MAX_SIZE = Number.parseInt(process.env.INGEST_QUEUE_MAX_SIZE || '20000', 10);
// Cantidad de iteraciones de bucle usada por la simulacion CPU en worker threads.
const CPU_SIMULATION_ITERATIONS = Number.parseInt(
  process.env.CPU_SIMULATION_ITERATIONS || '30000000',
  10
);
// Probabilidad de fallo simulado por request valida de ingest (0.0 a 1.0).
const INGEST_CRASH_PROBABILITY = Number.parseFloat(
  process.env.INGEST_CRASH_PROBABILITY || '0'
);

// Contrato de mensajes IPC entre procesos worker, worker threads y master.
const MESSAGE_TYPES = Object.freeze({
  INGEST_TASK: 'ingest:task',
  INGEST_DONE: 'ingest:done',
  INGEST_ERROR: 'ingest:error',
  STATS_REQUEST: 'stats:request',
  STATS_RESPONSE: 'stats:response',
});

// Endpoints HTTP publicos expuestos por la API.
const ROUTES = Object.freeze({
  HEALTH: '/health',
  INGEST: '/ingest',
  STATS: '/stats',
});

module.exports = {
  PORT,
  SHARED_COUNTER_BYTES,
  WORKER_FRACTION,
  INGEST_QUEUE_MAX_SIZE,
  CPU_SIMULATION_ITERATIONS,
  INGEST_CRASH_PROBABILITY,
  MESSAGE_TYPES,
  ROUTES,
};
