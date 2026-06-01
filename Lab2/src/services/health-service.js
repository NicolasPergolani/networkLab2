// Devuelve un payload liviano de liveness; aqui no se permiten operaciones costosas.
function getHealthStatus() {
  return {
    status: 'ok',
    pid: process.pid,
  };
}

module.exports = {
  getHealthStatus,
};
