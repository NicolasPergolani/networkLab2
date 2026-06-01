const path = require('path');
const { Worker } = require('worker_threads');
const { INGEST_QUEUE_MAX_SIZE, MESSAGE_TYPES } = require('../shared/constants');
const logger = require('../utils/logger');

// Executor de un solo thread por proceso worker del cluster.
class SingleThreadPool {
  constructor() {
    // Cola FIFO para tareas de ingest aceptadas esperando ejecucion del thread.
    this.queue = [];
    // Indica si el worker thread esta ejecutando una tarea actualmente.
    this.isBusy = false;
    // Cuando es true, se detiene el procesamiento de cola (usado en shutdown).
    this.stopping = false;
    // Rastrea la tarea en vuelo para reintento si el thread sale inesperadamente.
    this.currentTask = null;
    this.worker = null;
    // Capacidad de cola por worker usada como mecanismo de backpressure.
    this.maxQueueSize = Number.isInteger(INGEST_QUEUE_MAX_SIZE) && INGEST_QUEUE_MAX_SIZE > 0
      ? INGEST_QUEUE_MAX_SIZE
      : 20000;
    this.createWorker();
  }

  // Crea el Worker Thread dedicado y enlaza handlers de ciclo de vida.
  createWorker() {
    this.worker = new Worker(path.join(__dirname, 'ingest-thread.js'));

    this.worker.on('message', (message) => {
      if (!message || typeof message !== 'object') {
        return;
      }

      if (message.type === MESSAGE_TYPES.INGEST_DONE) {
        // Notifica al master via IPC de proceso que una ingest fue procesada.
        if (typeof process.send === 'function') {
          process.send({ type: MESSAGE_TYPES.INGEST_DONE, id: message.id });
        }

        this.isBusy = false;
        this.currentTask = null;
        this.processQueue();
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
function createThreadPool() {
  return new SingleThreadPool();
}

module.exports = {
  createThreadPool,
};
