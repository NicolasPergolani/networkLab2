const { ROUTES } = require('../shared/constants');
const { getHealthStatus } = require('../services/health-service');
const { acceptIngest } = require('../services/ingest-service');
const { getStats } = require('../services/stats-service');

// Helper pequeno para mantener formato JSON consistente en todas las rutas.
function sendJson(res, statusCode, payload) {
  res.statusCode = statusCode;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.end(JSON.stringify(payload));
}

// Router para todos los endpoints HTTP expuestos por un worker del cluster.
async function routeRequest(req, res, dependencies) {
  const method = req.method || 'GET';
  const url = new URL(req.url || '/', 'http://127.0.0.1');

  // El endpoint de health debe ser rapido y sin efectos secundarios.
  if (method === 'GET' && url.pathname === ROUTES.HEALTH) {
    sendJson(res, 200, getHealthStatus());
    return;
  }

  // El endpoint de ingest valida y encola trabajo pesado en un worker thread.
  if (method === 'GET' && url.pathname === ROUTES.INGEST) {
    const idParam = url.searchParams.get('id');
    const result = acceptIngest(idParam, dependencies.threadPool);

    if (!result.ok) {
      sendJson(res, result.statusCode || 400, {
        error: result.error,
        queueSize: result.queueSize,
        maxQueueSize: result.maxQueueSize,
      });
      return;
    }

    sendJson(res, result.statusCode || 202, {
      accepted: true,
      id: result.id,
      queueSize: result.queueSize,
      maxQueueSize: result.maxQueueSize,
    });
    return;
  }

  // El endpoint de stats obtiene el contador global via IPC del cluster.
  if (method === 'GET' && url.pathname === ROUTES.STATS) {
    const stats = await getStats();
    sendJson(res, 200, stats);
    return;
  }

  sendJson(res, 404, { error: 'not_found' });
}

module.exports = {
  routeRequest,
};
