const { PORT } = require('../shared/constants');
const { createSharedCounter } = require('../shared/shared-counter');
const logger = require('../utils/logger');
const { createHttpServer } = require('./http-server');
const { createThreadPool } = require('../threads/thread-pool');

// Inicializa los recursos propios de un proceso worker del cluster.
function startWorkerProcess() {
  // Contador compartido entre este proceso worker y su worker thread dedicado.
  const { buffer: localCounterBuffer } = createSharedCounter();
  // Executor en segundo plano para tareas de ingesta CPU-bound.
  const threadPool = createThreadPool({ sharedCounterBuffer: localCounterBuffer });
  // Servidor HTTP que delega ruteo y logica de negocio.
  const server = createHttpServer({ threadPool });

  server.listen(PORT, () => {
    logger.info('HTTP server listening', { port: PORT });
  });

  server.on('error', (error) => {
    logger.error('HTTP server error', { message: error.message });
    process.exit(1);
  });

  // Apagado ordenado: deja de aceptar conexiones y luego termina el thread.
  function shutdown(signal) {
    logger.warn('Worker shutting down', { signal });

    server.close(() => {
      threadPool.close().finally(() => {
        process.exit(0);
      });
    });
  }

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  // Guardas fail-fast: permiten que el master reemplace workers no saludables.
  process.on('uncaughtException', (error) => {
    logger.error('Uncaught exception in cluster worker', { message: error.message });
    process.exit(1);
  });

  process.on('unhandledRejection', (reason) => {
    logger.error('Unhandled rejection in cluster worker', {
      reason: reason instanceof Error ? reason.message : String(reason),
    });
    process.exit(1);
  });
}

module.exports = {
  startWorkerProcess,
};
