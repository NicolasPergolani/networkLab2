const os = require('os');
const { WORKER_FRACTION } = require('../shared/constants');

// Parsea enteros positivos de forma segura, devolviendo fallback si son invalidos.
function safePositiveInt(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed;
}

// Devuelve la cantidad de CPUs logicas disponibles en la maquina actual.
function getCpuCount() {
  return safePositiveInt(os.cpus().length, 1);
}

// Calcula la cantidad de workers del cluster segun la fraccion de CPU configurada.
function getClusterWorkerCount() {
  const cpuCount = getCpuCount();
  return Math.max(1, Math.floor(cpuCount * WORKER_FRACTION));
}

module.exports = {
  safePositiveInt,
  getCpuCount,
  getClusterWorkerCount,
};
