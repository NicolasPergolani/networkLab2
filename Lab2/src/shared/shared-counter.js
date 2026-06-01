const { SHARED_COUNTER_BYTES } = require('./constants');

// Reserva memoria compartida para un unico contador atomico Int32.
function createSharedCounter() {
  const buffer = new SharedArrayBuffer(SHARED_COUNTER_BYTES);
  const counter = new Int32Array(buffer);
  return { buffer, counter };
}

// Construye una vista Int32 segura sobre un SharedArrayBuffer existente.
function counterFromBuffer(buffer) {
  if (!(buffer instanceof SharedArrayBuffer)) {
    throw new TypeError('counterFromBuffer expects a SharedArrayBuffer');
  }

  if (buffer.byteLength < SHARED_COUNTER_BYTES) {
    throw new RangeError('Shared buffer is too small for Int32 counter');
  }

  return new Int32Array(buffer);
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
  counterFromBuffer,
  incrementCounter,
  readCounter,
};
