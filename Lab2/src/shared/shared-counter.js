const { SHARED_COUNTER_BYTES } = require('./constants');

// Reserva memoria compartida para un unico contador atomico Int32.
function createSharedCounter() {
  const buffer = new SharedArrayBuffer(SHARED_COUNTER_BYTES);
  const counter = new Int32Array(buffer);
  return { buffer, counter };
}

// Ejecuta un incremento atomico seguro para threads y procesos.
function incrementCounter(counter, delta) {
  const safeDelta = Number.isInteger(delta) && delta > 0 ? delta : 1;
  return Atomics.add(counter, 0, safeDelta);
}

// Lee el valor actual del contador de forma atomica.
function readCounter(counter) {
  return Atomics.load(counter, 0);
}

module.exports = {
  createSharedCounter,
  incrementCounter,
  readCounter,
};
