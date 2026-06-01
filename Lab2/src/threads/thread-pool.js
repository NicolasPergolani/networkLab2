const path = require('path');
const { Worker } = require('worker_threads');
const {
  INGEST_QUEUE_MAX_SIZE,
  INGEST_CRASH_PROBABILITY,
  MESSAGE_TYPES,
} = require('../shared/constants');
const { counterFromBuffer, readCounter } = require('../shared/shared-counter');
const logger = require('../utils/logger');

// Executor de un solo thread por proceso worker del cluster.
class SingleThreadPool {
  constructor(options = {}) {
    // Cola FIFO para tareas de ingest aceptadas esperando ejecucion del thread.
    this.queue = [];
    // Indica si el worker thread esta ejecutando una tarea actualmente.
    this.isBusy = false;
    // Cuando es true, se detiene el procesamiento de cola (usado en shutdown).
    this.stopping = false;
    // Rastrea la tarea en vuelo para reintento si el thread sale inesperadamente.
    this.currentTask = null;
    this.worker = null;
    this.sharedCounter = counterFromBuffer(options.sharedCounterBuffer);
    this.crashProbability = Number.isFinite(INGEST_CRASH_PROBABILITY)
      ? Math.min(1, Math.max(0, INGEST_CRASH_PROBABILITY))
      : 0;
    // Se programa a lo sumo un crash por ciclo de vida del worker de cluster.
    this.postProcessingCrashScheduled = false;
    // Capacidad de cola por worker usada como mecanismo de backpressure.
    this.maxQueueSize = Number.isInteger(INGEST_QUEUE_MAX_SIZE) && INGEST_QUEUE_MAX_SIZE > 0
      ? INGEST_QUEUE_MAX_SIZE
      : 20000;
    this.createWorker();
  }

  // Crea el Worker Thread dedicado y enlaza handlers de ciclo de vida.
  createWorker() {
    this.worker = new Worker(path.join(__dirname, 'ingest-thread.js'), {
      workerData: {
        sharedCounterBuffer: this.sharedCounter.buffer,
      },
    });

    this.worker.on('message', (message) => {
      if (!message || typeof message !== 'object') {
        return;
      }

      if (message.type === MESSAGE_TYPES.INGEST_DONE) {
        // Notifica al master via IPC de proceso que una ingest fue procesada.
        if (typeof process.send === 'function') {
          process.send({
            type: MESSAGE_TYPES.INGEST_DONE,
            id: message.id,
            localProcessedCount: readCounter(this.sharedCounter),
          });
        }

        this.isBusy = false;
        this.currentTask = null;

        // Simula caida solo despues de procesar tareas, nunca antes de aceptar la request.
        if (
          !this.postProcessingCrashScheduled
          && this.crashProbability > 0
          && Math.random() < this.crashProbability
        ) {
          this.postProcessingCrashScheduled = true;
        }

        this.processQueue();
        this.maybeCrashAfterQueueDrain();
        return;
      }

      if (message.type === MESSAGE_TYPES.INGEST_ERROR) {
        logger.error('Ingest thread reported an error', {
          taskId: message.id,
          message: message.error,
        });

        // Continua con la siguiente tarea en cola aun si una tarea individual falla.
        this.isBusy = false;
        this.currentTask = null;
        this.processQueue();
      }
    });

    this.worker.on('error', (error) => {
      logger.error('Ingest worker thread crashed', { message: error.message });
    });

    this.worker.on('exit', (code) => {
      if (this.stopping) {
        return;
      }

      logger.warn('Ingest worker thread exited unexpectedly; recreating', { code });

      // Reinyecta la tarea en vuelo para evitar perder trabajo aceptado ante salida abrupta.
      if (this.currentTask) {
        this.queue.unshift(this.currentTask);
        this.currentTask = null;
      }

      this.isBusy = false;
      this.createWorker();
      this.processQueue();
    });
  }

  // Fuerza una caida simulada solo cuando no quedan tareas aceptadas pendientes.
  maybeCrashAfterQueueDrain() {
    if (this.stopping || !this.postProcessingCrashScheduled) {
      return;
    }

    if (this.isBusy || this.queue.length > 0) {
      return;
    }

    setImmediate(() => {
      if (this.stopping || this.isBusy || this.queue.length > 0) {
        return;
      }

      process.stderr.write(
        `[CRASH SIMULADO] Worker PID=${process.pid} cae despues de procesar su cola.\n`
      );
      process.exit(1);
    });
  }

  // Agrega una tarea de ingest a la cola salvo que se alcance el limite de backpressure.
  enqueue(id) {
    if (this.queue.length >= this.maxQueueSize) {
      return {
        ok: false,
        reason: 'queue_full',
        queueSize: this.queue.length,
        maxQueueSize: this.maxQueueSize,
      };
    }

    this.queue.push({ id });
    this.processQueue();

    return {
      ok: true,
      // Reporta profundidad aproximada de cola incluyendo tarea en vuelo actual.
      queueSize: this.queue.length + (this.isBusy ? 1 : 0),
      maxQueueSize: this.maxQueueSize,
    };
  }

  // Despacha una tarea en cola si el worker thread esta ocioso.
  processQueue() {
    if (this.stopping || this.isBusy || this.queue.length === 0) {
      return;
    }

    const nextTask = this.queue.shift();
    this.currentTask = nextTask;
    this.isBusy = true;

    try {
      this.worker.postMessage({
        type: MESSAGE_TYPES.INGEST_TASK,
        id: nextTask.id,
      });
    } catch (error) {
      // Reencola la tarea si postMessage falla, preservando semantica de al-menos-un-intento.
      logger.error('Failed to dispatch task to worker thread', { message: error.message });
      this.queue.unshift(nextTask);
      this.currentTask = null;
      this.isBusy = false;
    }
  }

  // Detiene el procesamiento de cola y termina el worker thread.
  async close() {
    this.stopping = true;

    if (!this.worker) {
      return;
    }

    try {
      await this.worker.terminate();
    } catch (error) {
      logger.error('Failed to terminate worker thread cleanly', { message: error.message });
    }
  }
}

// Fabrica publica para crear la instancia de cola/worker thread por proceso.
function createThreadPool(options = {}) {
  return new SingleThreadPool(options);
}

module.exports = {
  createThreadPool,
};
