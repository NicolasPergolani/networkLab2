const { isMainThread, parentPort } = require('worker_threads');
const { CPU_SIMULATION_ITERATIONS, MESSAGE_TYPES } = require('../shared/constants');

// Simulacion CPU intensiva deterministica usada para demostrar paralelismo real.
function simulateCpuWork(id, iterations) {
  let checksum = 0;
  const totalIterations = Number.isInteger(iterations) && iterations > 0 ? iterations : 1;

  for (let step = 0; step < totalIterations; step += 1) {
    checksum = (checksum + ((id + step) % 97) * 2654435761) >>> 0;
  }

  return checksum;
}

// Este script se ejecuta dentro del contexto de un Worker Thread.
if (!isMainThread) {
  // Recibe tareas de ingest desde el proceso worker del cluster que lo contiene.
  parentPort.on('message', (message) => {
    if (!message || message.type !== MESSAGE_TYPES.INGEST_TASK) {
      return;
    }

    try {
      const id = Number(message.id);
      // El bucle pesado corre intencionalmente fuera del event loop HTTP.
      const checksum = simulateCpuWork(id, CPU_SIMULATION_ITERATIONS);

      // Notifica finalizacion para que el proceso actualice cola y metricas.
      parentPort.postMessage({
        type: MESSAGE_TYPES.INGEST_DONE,
        id,
        checksum,
      });
    } catch (error) {
      // Reporta errores del worker thread de vuelta al proceso.
      parentPort.postMessage({
        type: MESSAGE_TYPES.INGEST_ERROR,
        id: message.id,
        error: error.message,
      });
    }
  });
}

module.exports = {
  simulateCpuWork,
};
