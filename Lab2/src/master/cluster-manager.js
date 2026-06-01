const cluster = require('cluster');
const { getCpuCount, getClusterWorkerCount } = require('../utils/cpu-utils');
const { createSharedCounter, incrementCounter, readCounter } = require('../shared/shared-counter');
const { MESSAGE_TYPES } = require('../shared/constants');
const logger = require('../utils/logger');

// Ventana de tiempo usada para controlar cuantas veces se reinician workers.
const RESTART_WINDOW_MS = 30000;
// Limite duro para reducir la probabilidad de bucles infinitos de fallo/reinicio.
const MAX_RESTARTS_PER_WINDOW = 20;

// Conecta eventos IPC desde un worker de cluster hacia responsabilidades del master.
function attachWorkerMessageHandler(worker, counter) {
  worker.on('message', (message) => {
    if (!message || typeof message !== 'object') {
      return;
    }

    // Solo el master incrementa el contador global, garantizando una autoridad unica.
    if (message.type === MESSAGE_TYPES.INGEST_DONE) {
      incrementCounter(counter, 1);
      return;
    }

    // /stats se responde desde el master para mantener un valor global consistente.
    if (message.type === MESSAGE_TYPES.STATS_REQUEST) {
      worker.send({
        type: MESSAGE_TYPES.STATS_RESPONSE,
        requestId: message.requestId,
        processedEvents: readCounter(counter),
      });
    }
  });
}

// Crea un worker del cluster y adjunta todos los handlers necesarios.
function forkWorker(counter) {
  const worker = cluster.fork();
  attachWorkerMessageHandler(worker, counter);
  return worker;
}

// Conserva solo timestamps que siguen dentro de la ventana de control de reinicios.
function pruneRestartTimestamps(restartTimestamps, now) {
  const threshold = now - RESTART_WINDOW_MS;

  while (restartTimestamps.length > 0 && restartTimestamps[0] < threshold) {
    restartTimestamps.shift();
  }
}

// Inicializa workers del cluster y maneja eventos de resiliencia.
function startCluster() {
  const cpuCount = getCpuCount();
  // Requisito del TP: usar la mitad de los nucleos de CPU disponibles.
  const targetWorkers = getClusterWorkerCount();
  // Contador atomico compartido que registra eventos procesados de forma global.
  const { counter } = createSharedCounter();
  // Ventana deslizante de timestamps para proteger contra loops de reinicio.
  const restartTimestamps = [];

  logger.info('Master initialized', {
    cpuCount,
    targetWorkers,
    strategy: 'half-cpus',
  });

  for (let index = 0; index < targetWorkers; index += 1) {
    const worker = forkWorker(counter);
    logger.info('Worker forked', { workerId: worker.id, workerPid: worker.process.pid });
  }

  cluster.on('online', (worker) => {
    logger.info('Worker online', { workerId: worker.id, workerPid: worker.process.pid });
  });

  // Auto-recuperacion: recrea workers que finalizan de forma inesperada.
  cluster.on('exit', (deadWorker, code, signal) => {
    const now = Date.now();
    pruneRestartTimestamps(restartTimestamps, now);

    const abruptExit = deadWorker.exitedAfterDisconnect !== true;

    logger.error('Worker exited; creating replacement', {
      deadWorkerId: deadWorker.id,
      deadWorkerPid: deadWorker.process.pid,
      code,
      signal: signal || 'none',
      abruptExit,
    });

    if (!abruptExit) {
      logger.warn('Worker exited intentionally; replacement skipped', {
        deadWorkerId: deadWorker.id,
      });
      return;
    }

    // Si la tasa de fallos es muy alta, retrasa el reemplazo para evitar loops cerrados.
    if (restartTimestamps.length >= MAX_RESTARTS_PER_WINDOW) {
      logger.error('Restart rate limit reached; delaying replacement', {
        maxRestarts: MAX_RESTARTS_PER_WINDOW,
        windowMs: RESTART_WINDOW_MS,
      });

      setTimeout(() => {
        const replacement = forkWorker(counter);
        logger.info('Replacement worker created after cooldown', {
          deadWorkerId: deadWorker.id,
          replacementWorkerId: replacement.id,
          replacementPid: replacement.process.pid,
        });
      }, 1000);
      return;
    }

    // Camino normal: registrar reinicio y reemplazar inmediatamente.
    restartTimestamps.push(now);

    const replacement = forkWorker(counter);
    logger.info('Replacement worker created', {
      deadWorkerId: deadWorker.id,
      replacementWorkerId: replacement.id,
      replacementPid: replacement.process.pid,
    });
  });
}

module.exports = {
  startCluster,
};
