const cluster = require('cluster');
const { getCpuCount, getClusterWorkerCount } = require('../utils/cpu-utils');
const { createSharedCounter, incrementCounter, readCounter } = require('../shared/shared-counter');
const { MESSAGE_TYPES, INGEST_CRASH_PROBABILITY } = require('../shared/constants');
const logger = require('../utils/logger');

// Ventana de tiempo usada para controlar cuantas veces se reinician workers.
const RESTART_WINDOW_MS = 30000;
// Limite duro para reducir la probabilidad de bucles infinitos de fallo/reinicio.
const MAX_RESTARTS_PER_WINDOW = 20;

// Conecta eventos IPC desde un worker de cluster hacia responsabilidades del master.
function attachWorkerMessageHandler(worker, counter, perWorkerProcessed) {
  worker.on('message', (message) => {
    if (!message || typeof message !== 'object') {
      return;
    }

    // Solo el master incrementa el contador global, garantizando una autoridad unica.
    if (message.type === MESSAGE_TYPES.INGEST_DONE) {
      incrementCounter(counter, 1);

      const workerPid = String(worker.process?.pid || worker.id);
      const current = perWorkerProcessed.get(workerPid) || 0;
      perWorkerProcessed.set(workerPid, current + 1);
      return;
    }

    // /stats se responde desde el master para mantener un valor global consistente.
    if (message.type === MESSAGE_TYPES.STATS_REQUEST) {
      worker.send({
        type: MESSAGE_TYPES.STATS_RESPONSE,
        requestId: message.requestId,
        processedEvents: readCounter(counter),
        processedByWorker: Object.fromEntries(perWorkerProcessed),
      });
    }
  });
}

// Crea un worker del cluster y adjunta todos los handlers necesarios.
function forkWorker(counter, perWorkerProcessed) {
  const worker = cluster.fork();
  const workerPid = String(worker.process?.pid || worker.id);

  if (!perWorkerProcessed.has(workerPid)) {
    perWorkerProcessed.set(workerPid, 0);
  }

  attachWorkerMessageHandler(worker, counter, perWorkerProcessed);
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
  // Conteo acumulado por proceso worker para inspeccionar distribucion real.
  const perWorkerProcessed = new Map();
  // Ventana deslizante de timestamps para proteger contra loops de reinicio.
  const restartTimestamps = [];

  logger.info('Master initialized', {
    cpuCount,
    targetWorkers,
    strategy: 'half-cpus',
    ingestCrashProbability: INGEST_CRASH_PROBABILITY,
  });

  if (INGEST_CRASH_PROBABILITY > 0) {
    logger.warn('Crash simulation enabled', {
      ingestCrashProbability: INGEST_CRASH_PROBABILITY,
    });
  }

  for (let index = 0; index < targetWorkers; index += 1) {
    const worker = forkWorker(counter, perWorkerProcessed);
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

    const bar = '='.repeat(60);
    process.stderr.write(`\n${bar}\n`);
    process.stderr.write(
      `[SELF-HEALING] Worker #${deadWorker.id} (PID ${deadWorker.process.pid}) ` +
      `murio (code=${code}, signal=${signal || 'none'}).\n`
    );
    process.stderr.write(`${bar}\n\n`);

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
        const replacement = forkWorker(counter, perWorkerProcessed);
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

    const replacement = forkWorker(counter, perWorkerProcessed);
    process.stderr.write(
      `[SELF-HEALING] Reemplazo lanzado: Worker #${replacement.id} ` +
      `(PID ${replacement.process.pid}) -> API sigue online.\n\n`
    );
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
