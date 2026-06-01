const cluster = require('cluster');
const { startCluster } = require('./master/cluster-manager');
const { startWorkerProcess } = require('./worker/worker-process');

// Punto de entrada: decide si este proceso actua como master del cluster o como worker HTTP.
function bootstrap() {
  // El proceso primario solo se encarga de la orquestacion de procesos.
  if (cluster.isPrimary) {
    // Round-robin explicito para repartir mejor las conexiones entre workers.
    cluster.schedulingPolicy = cluster.SCHED_RR;
    startCluster();
    return;
  }

  // Los workers creados con fork ejecutan la API y delegan trabajo pesado en threads de trabajo.
  startWorkerProcess();
}

bootstrap();
