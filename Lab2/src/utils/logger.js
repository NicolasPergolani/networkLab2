// Formatea metadatos opcionales como JSON compacto al final de una linea de log.
function formatMeta(meta) {
  if (!meta || Object.keys(meta).length === 0) {
    return '';
  }

  return ` ${JSON.stringify(meta)}`;
}

// Logger unificado usado en master/workers para observabilidad consistente.
function log(level, message, meta) {
  const timestamp = new Date().toISOString();
  const pid = process.pid;
  const upperLevel = String(level || 'INFO').toUpperCase();

  // Mantiene logs en una sola linea para parseo mas simple en entornos con cluster.
  console.log(`[${timestamp}] [PID:${pid}] [${upperLevel}] ${message}${formatMeta(meta)}`);
}

function info(message, meta) {
  log('info', message, meta);
}

function warn(message, meta) {
  log('warn', message, meta);
}

function error(message, meta) {
  log('error', message, meta);
}

module.exports = {
  info,
  warn,
  error,
};
