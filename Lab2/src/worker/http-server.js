const http = require('http');
const { routeRequest } = require('./routes');
const logger = require('../utils/logger');

// Crea un servidor HTTP minimalista; la logica de endpoints vive en routes/services.
function createHttpServer(dependencies) {
  return http.createServer((req, res) => {
    // Borde de error asincrono centralizado para todos los handlers.
    routeRequest(req, res, dependencies).catch((error) => {
      logger.error('Unhandled route error', { message: error.message });

      if (res.headersSent) {
        res.end();
        return;
      }

      res.statusCode = 500;
      res.setHeader('Content-Type', 'application/json; charset=utf-8');
      res.end(JSON.stringify({ error: 'internal_error' }));
    });
  });
}

module.exports = {
  createHttpServer,
};
