'use strict';

const assert = require('assert');

const { AmqpConnection } = require('../lib/amqp-connection');
const { AmqpQueue } = require('../lib/amqp-queue');
const gmq = require('..');
const { Events, Status } = require('../lib/constants');
const { MqttConnection } = require('../lib/mqtt-connection');
const { MqttQueue } = require('../lib/mqtt-queue');

const RETRY_10MS = 100;

/**
 * @typedef {Object} Engine
 * @property {AmqpConnection|MqttConnection} Connection
 * @property {AmqpQueue|MqttQueue} Queue
 */

function afterEach(done) {
  let withErr = null;
  (function closeConnFn() {
    const conn = module.exports.conn.pop();
    if (!conn) {
      return void done(withErr);
    }
    conn.close((err) => {
      if (err) {
        withErr = err;
      }
      closeConnFn();
    });
  })();
}

/**
 * Test default options.
 *
 * @param {Engine} engine
 */
function newDefault(engine) {
  return function () {
    assert.ok(new engine.Connection());
  };
}

/**
 * Test zero value options.
 *
 * @param {Engine} engine
 */
function newZero(engine) {
  return function () {
    const opts = {
      uri: engine === gmq.amqp ? 'amqp://localhost' : 'mqtt://localhost',
      connectTimeoutMillis: 0,
      reconnectMillis: 0,
      insecure: false,
    };
    assert.ok(new engine.Connection(opts));
  };
}

/**
 * Test options with wrong values.
 *
 * @param {Engine} engine
 */
function newWrongOpts(engine) {
  return function () {
    assert.throws(() => {
      new engine.Connection(null);
    });
    assert.throws(() => {
      new engine.Connection({ uri: 1 });
    });
    assert.throws(() => {
      new engine.Connection({ uri: ':://' });
    });
    assert.throws(() => {
      new engine.Connection({ uri: 'amq://localhost' });
    });
    assert.throws(() => {
      new engine.Connection({ connectTimeoutMillis: 0.1 });
    });
    assert.throws(() => {
      new engine.Connection({ reconnectMillis: 0.1 });
    });
    assert.throws(() => {
      new engine.Connection({ insecure: 0 });
    });

    if (engine === gmq.mqtt) {
      assert.throws(() => {
        new engine.Connection({ clientId: '012345678901234567890123' });
      });
      assert.throws(() => {
        new engine.Connection({ cleanSession: 0 });
      });
    }
  };
}

/**
 * Test connection properties after `new`.
 *
 * @param {Engine} engine
 */
function properties(engine) {
  return function () {
    const conn = new engine.Connection();
    assert.ok(conn);

    assert.strictEqual(conn.status(), Status.Closed);
  };
}

/**
 * Test `connect()` without handlers.
 *
 * @param {Engine} engine
 */
function connectNoHandler(engine) {
  return function (done) {
    const conn = new engine.Connection();
    assert.ok(conn);

    module.exports.conn.push(conn);
    conn.connect();
    waitConnected(conn, function (err) {
      done(err);
    });
  };
}

/**
 * Test `connect()` with a handler.
 *
 * @param {Engine} engine
 */
function connectWithHandler(engine) {
  return function (done) {
    const conn = new engine.Connection();
    assert.ok(conn);
    conn.on(Events.Status, (status) => {
      if (status === Status.Connected) {
        done(null);
      }
    });

    module.exports.conn.push(conn);
    conn.connect();
  };
}

/**
 * Test `connect()` for a conneted connection.
 *
 * @param {Engine} engine
 */
function connectAfterConnect(engine) {
  return function (done) {
    const conn = new engine.Connection();
    assert.ok(conn);
    conn.on(Events.Status, (status) => {
      if (status === Status.Connected) {
        conn.connect();
        done(null);
      }
    });

    module.exports.conn.push(conn);
    conn.connect();
  };
}

/**
 * Test `connect()` with an insecure TLS connection.
 *
 * @param {Engine} engine
 */
function connectInsecure(engine) {
  return function (done) {
    const conn = new engine.Connection({
      uri: engine === gmq.amqp ? 'amqps://localhost' : 'mqtts://localhost',
      insecure: true,
    });
    assert.ok(conn);
    conn.on(Events.Status, (status) => {
      if (status === Status.Connected) {
        done(null);
      }
    });

    module.exports.conn.push(conn);
    conn.connect();
  };
}

/**
 * Test `close()`.
 *
 * @param {Engine} engine
 */
function close(engine) {
  return function (done) {
    const conn = new engine.Connection();
    assert.ok(conn);
    let recvConnected = false;
    conn.on(Events.Status, (status) => {
      if (status === Status.Connected) {
        recvConnected = true;
        conn.close((err) => {
          if (err) {
            done(err);
          } else if (conn.status() !== Status.Closed) {
            done(Error(`closed with status ${conn.status()}`));
          }
        });
      } else if (status === Status.Closed) {
        if (recvConnected) {
          if (conn.status() !== Status.Closed) {
            return void done(Error(`closed with status ${conn.status()}`));
          }
          done(null);
        }
      }
    });

    module.exports.conn.push(conn);
    conn.connect();
  };
}

/**
 * Test `close()` for a closed connection.
 *
 * @param {Engine} engine
 */
function closeAfterClose(engine) {
  return function (done) {
    const conn = new engine.Connection();
    assert.ok(conn);
    let recvConnected = false;
    conn.on(Events.Status, (status) => {
      if (status === Status.Connected) {
        recvConnected = true;
        conn.close((err) => {
          if (err) {
            done(err);
          }
        });
      } else if (status === Status.Closed) {
        if (recvConnected) {
          conn.close(done);
        }
      }
    });

    module.exports.conn.push(conn);
    conn.connect();
  };
}

/**
 * Test `close()` without callback.
 *
 * @param {Engine} engine
 */
function closeNoCallback(engine) {
  return function (done) {
    const conn = new engine.Connection();
    assert.ok(conn);
    let recvConnected = false;
    conn.on(Events.Status, (status) => {
      if (status === Status.Connected) {
        recvConnected = true;
        conn.close();
      } else if (status === Status.Closed) {
        if (recvConnected) {
          conn.close();
          done(null);
        }
      }
    });

    module.exports.conn.push(conn);
    conn.connect();
  };
}

/**
 * @param {AmqpConnection|MqttConnection} conn
 * @param {function} callback
 *   @param {?Error} callback.err
 */
function waitConnected(conn, callback) {
  let retry = RETRY_10MS;
  const waitFn = function () {
    if (retry < 0) {
      return void callback(Error('not connected'));
    } else if (conn.status() === Status.Connected) {
      return void callback(null);
    }
    retry--;
    setTimeout(waitFn, 10);
  };
  setTimeout(waitFn, 10);
}

module.exports = {
  conn: [],

  afterEach,
  newDefault,
  newZero,
  newWrongOpts,
  properties,
  connectNoHandler,
  connectWithHandler,
  connectAfterConnect,
  connectInsecure,
  close,
  closeAfterClose,
  closeNoCallback,
};
