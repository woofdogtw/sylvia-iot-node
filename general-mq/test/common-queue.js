'use strict';

const assert = require('assert');

const async = require('async');

const { AmqpConnection } = require('../lib/amqp-connection');
const { AmqpQueue, AmqpQueueMsgHandler, AmqpMessage } = require('../lib/amqp-queue');
const gmq = require('..');
const { Errors, Events, Status } = require('../lib/constants');
const { MqttConnection } = require('../lib/mqtt-connection');
const { MqttQueue, MqttQueueMsgHandler, MqttMessage } = require('../lib/mqtt-queue');

const RETRY_10MS = 100;

class TestRecvMsgHandler {
  constructor() {
    this.recvMessages = [];
    this.ackErrors = [];
    this.useNack = false;
    this.nackMessages = [];
    this.nackErrors = [];
  }

  /**
   * The handler for `Queue.setMsgHandler()`.
   *
   * @param {AmqpQueue|MqttQueue} queue
   * @param {AmqpMessage|MqttMessage} msg
   */
  onMessage(queue, msg) {
    const self = this;
    if (this.useNack) {
      queue.nack(msg, (err) => {
        if (err) {
          self.nackErrors.push(err);
        } else {
          self.nackMessages.push(msg.payload);
        }
      });
    } else {
      queue.ack(msg, (err) => {
        if (err) {
          self.ackErrors.push(err);
        } else {
          self.recvMessages.push(msg.payload);
        }
      });
    }
  }

  /** @type {Buffer[]} */
  recvMessages;
  /** @type {Error[]} */
  ackErrors;
  /** @type {boolean} */
  useNack;
  /** @type {Buffer[]} */
  nackMessages;
  /** @type {Error[]} */
  nackErrors;
}

/**
 * @typedef {Object} Engine
 * @property {AmqpConnection|MqttConnection} Connection
 * @property {AmqpQueue|MqttQueue} Queue
 */

function afterEach(done) {
  let withErr = null;

  (function closeQueueFn() {
    const queue = module.exports.queues.pop();
    if (!queue) {
      return void closeConnFn();
    }
    queue.close((err) => {
      if (err) {
        withErr = err;
      }
      closeQueueFn();
    });
  })();

  function closeConnFn() {
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
  }
}

/**
 * Test default options.
 *
 * @param {Engine} engine
 */
function newDefault(engine) {
  return function () {
    const conn = new engine.Connection();
    assert.ok(conn);

    const opts = {
      name: 'name',
      isRecv: false,
      reliable: false,
      broadcast: false,
    };
    assert.ok(new engine.Queue(opts, conn));
  };
}

/**
 * Test zero value options.
 *
 * @param {Engine} engine
 */
function newZero(engine) {
  return function () {
    const conn = new engine.Connection();
    assert.ok(conn);

    const opts = {
      name: 'name',
      isRecv: false,
      reliable: false,
      broadcast: false,
      reconnectMillis: 0,
    };
    assert.ok(new engine.Queue(opts, conn));
  };
}

/**
 * Test options with wrong values.
 *
 * @param {Engine} engine
 */
function newWrongOpts(engine) {
  return function () {
    const conn = new engine.Connection();
    assert.ok(conn);

    assert.throws(() => {
      new engine.Queue(null, conn);
    });
    assert.throws(() => {
      const opts = {
        name: 'name',
        isRecv: false,
        reliable: false,
        broadcast: false,
      };
      const c = engine === gmq.amqp ? new gmq.mqtt.Connection() : new gmq.amqp.Connection();
      new engine.Queue(opts, c);
    });
    assert.throws(() => {
      const opts = {
        name: 'A@',
        isRecv: false,
        reliable: false,
        broadcast: false,
      };
      new engine.Queue(opts, conn);
    });
    assert.throws(() => {
      const opts = {
        name: 'name',
        isRecv: 0,
        reliable: false,
        broadcast: false,
      };
      new engine.Queue(opts, conn);
    });
    assert.throws(() => {
      const opts = {
        name: 'name',
        isRecv: false,
        reliable: 0,
        broadcast: false,
      };
      new engine.Queue(opts, conn);
    });
    assert.throws(() => {
      const opts = {
        name: 'name',
        isRecv: false,
        reliable: false,
        broadcast: false,
        reconnectMillis: 0.1,
      };
      new engine.Queue(opts, conn);
    });
    assert.throws(() => {
      const opts = {
        name: 'name',
        isRecv: false,
        reliable: false,
        broadcast: 0,
      };
      new engine.Queue(opts, conn);
    });

    if (engine === gmq.amqp) {
      assert.throws(() => {
        const opts = {
          name: 'name',
          isRecv: true,
          reliable: false,
          broadcast: false,
          prefetch: 0,
        };
        new engine.Queue(opts, conn);
      });
      assert.throws(() => {
        const opts = {
          name: 'name',
          isRecv: true,
          reliable: false,
          broadcast: false,
          prefetch: 1,
          persistent: 0,
        };
        new engine.Queue(opts, conn);
      });
    } else if (engine === gmq.mqtt) {
      assert.throws(() => {
        const opts = {
          name: 'name',
          isRecv: false,
          reliable: false,
          broadcast: false,
          sharedPrefix: 0,
        };
        new engine.Queue(opts, conn);
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

    const opts = {
      name: 'name',
      isRecv: false,
      reliable: false,
      broadcast: false,
      reconnectMillis: 0,
    };
    const queue = new engine.Queue(opts, conn);
    assert.ok(queue);
    assert.strictEqual(queue.name(), 'name');
    assert.strictEqual(queue.isRecv(), false);
    assert.strictEqual(queue.status(), Status.Closed);
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
    const opts = {
      name: 'name',
      isRecv: true,
      reliable: true,
      broadcast: false,
      prefetch: 1,
    };
    const queue = new engine.Queue(opts, conn);
    assert.ok(queue);
    assert.doesNotThrow(() => {
      queue.setMsgHandler((_queue, _msg) => {});
    });

    module.exports.conn.push(conn);
    module.exports.queues.push(queue);
    conn.connect();
    queue.connect();
    waitConnected(queue, function (err) {
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
    const opts = {
      name: 'name',
      isRecv: false,
      reliable: false,
      broadcast: true,
    };
    const queue = new engine.Queue(opts, conn);
    assert.ok(queue);
    queue.on(Events.Status, (status) => {
      if (status === Status.Connected) {
        done(null);
      }
    });

    module.exports.conn.push(conn);
    module.exports.queues.push(queue);
    conn.connect();
    queue.connect();
  };
}

/**
 * Test `connect()` for a conneted queue.
 *
 * @param {Engine} engine
 */
function connectAfterConnect(engine) {
  return function (done) {
    const conn = new engine.Connection();
    assert.ok(conn);
    const opts = {
      name: 'name',
      isRecv: false,
      reliable: false,
      broadcast: true,
    };
    const queue = new engine.Queue(opts, conn);
    assert.ok(queue);
    queue.on(Events.Status, (status) => {
      if (status === Status.Connected) {
        queue.connect();
        done(null);
      }
    });

    module.exports.conn.push(conn);
    module.exports.queues.push(queue);
    conn.connect();
    queue.connect();
  };
}

/**
 * To set.
 *
 * @param {Engine} engine
 */
function setMsgHandler(engine) {
  return function () {
    const conn = new engine.Connection();
    assert.ok(conn);

    const opts = {
      name: 'name',
      isRecv: false,
      reliable: false,
      broadcast: false,
    };
    const queue = new engine.Queue(opts, conn);
    assert.ok(queue);

    assert.doesNotThrow(() => {
      queue.setMsgHandler((_msg) => {});
    });
  };
}

/**
 * Test `close()`.
 *
 * @param {Engine} engine
 */
function close(engine) {
  return function (done) {
    let recvClosed = false;

    async.waterfall(
      [
        function (cb) {
          const handlers = {
            status: function (status) {
              if (status === Status.Closed) {
                recvClosed = true;
              }
            },
          };
          createConnRsc(engine, handlers, true, cb);
        },
        function (cb) {
          (function waitFn(index) {
            if (index >= module.exports.queues.length) {
              return void cb(null);
            }
            const q = module.exports.queues[index];
            waitConnected(q, (err) => {
              if (err) {
                return void cb(null);
              }
              waitFn(index + 1);
            });
          })(0);
        },
        function (cb) {
          const q = module.exports.queues[0];
          q.close((err) => {
            if (err) {
              return void cb(err);
            } else if (!recvClosed) {
              return void cb('not closed');
            }
            cb(null);
          });
        },
      ],
      done
    );
  };
}

/**
 * Test `close()` for a closed queue.
 *
 * @param {Engine} engine
 */
function closeAfterClose(engine) {
  return function (done) {
    async.waterfall(
      [
        function (cb) {
          createConnRsc(engine, {}, true, cb);
        },
        function (cb) {
          (function waitFn(index) {
            if (index >= module.exports.queues.length) {
              return void cb(null);
            }
            const q = module.exports.queues[index];
            waitConnected(q, (err) => {
              if (err) {
                return void cb(null);
              }
              waitFn(index + 1);
            });
          })(0);
        },
        function (cb) {
          const q = module.exports.queues[0];
          q.on(Events.Status, (status) => {
            if (status !== Status.Closed) {
              return;
            }
            q.close((err) => {
              if (err) {
                return void cb(err);
              }
              q.close();
              cb(null);
            });
          });
          q.close();
        },
      ],
      done
    );
  };
}

/**
 * Test send with an invalid queue.
 *
 * @param {Engine} engine
 */
function sendError(engine) {
  return function (done) {
    const conn = new engine.Connection();
    assert.ok(conn);
    const opts = {
      name: 'name',
      isRecv: true,
      reliable: false,
      broadcast: false,
      prefetch: 1,
    };
    const queue = new engine.Queue(opts, conn);
    assert.ok(queue);

    assert.throws(() => {
      queue.connect();
    });
    assert.throws(() => {
      queue.setMsgHandler({});
    });
    assert.doesNotThrow(() => {
      queue.setMsgHandler((_queue, _msg) => {});
    });
    assert.throws(() => {
      queue.sendMsg('payload');
    });
    assert.throws(() => {
      queue.sendMsg(Buffer.from(''), {});
    });

    async.waterfall(
      [
        function (cb) {
          queue.sendMsg(Buffer.alloc(0), (err) => {
            cb(err ? null : Error('send to not-connected queue should error'));
          });
        },
        function (cb) {
          queue.on(Events.Status, (status) => {
            if (status === Status.Connected) {
              queue.sendMsg(Buffer.alloc(0), (err) => {
                cb(err ? null : Error('send to recv queue should error'));
              });
            }
          });
          module.exports.conn.push(conn);
          module.exports.queues.push(queue);
          conn.connect();
          queue.connect();
        },
      ],
      done
    );
  };
}

/**
 * Test reconnect by closing/connecting the associated connection.
 *
 * @param {Engine} engine
 */
function reconnect(engine) {
  return function (done) {
    let recvConnecting = false;

    async.waterfall(
      [
        function (cb) {
          const handlers = {
            status: function (status) {
              if (status === Status.Connecting) {
                recvConnecting = true;
              }
            },
          };
          createConnRsc(engine, handlers, true, cb);
        },
        function (cb) {
          (function waitFn(index) {
            if (index >= module.exports.queues.length) {
              return void cb(null);
            }
            const q = module.exports.queues[index];
            waitConnected(q, (err) => {
              if (err) {
                return void cb(err);
              }
              waitFn(index + 1);
            });
          })(0);
        },
        function (cb) {
          const conn = module.exports.conn[0];
          if (!conn) {
            return void cb(Error('should have a connection'));
          }
          const queue = module.exports.queues[0];
          if (!queue) {
            return void cb(Error('should have a queue'));
          }
          recvConnecting = false;
          conn.close((err) => {
            if (err) {
              return void cb(Error(`close connection error: ${err}`));
            }
            const retry = 200;
            (function waitFn(index) {
              if (recvConnecting) {
                return void cb(null);
              } else if (index >= retry) {
                return void cb(Error('no connecting event'));
              }
              setTimeout(() => {
                waitFn(index + 1);
              }, 10);
            })(0);
          });
        },
        function (cb) {
          const conn = module.exports.conn[0];
          conn.connect();
          const queue = module.exports.queues[0];
          waitConnected(queue, cb);
        },
      ],
      done
    );
  };
}

/**
 * Send unicast data to one receiver.
 *
 * @param {Engine} engine
 */
function dataUnicast1to1(engine) {
  return function (done) {
    let handlers;

    async.waterfall(
      [
        function (cb) {
          const opts = {
            name: 'name',
            isRecv: false,
            reliable: false,
            broadcast: false,
            prefetch: 1,
            sharedPrefix: '$share/general-mq/',
          };
          createMsgRsc(engine, opts, 1, (err, h) => {
            if (err) {
              return void cb(err);
            }
            handlers = h;
            cb(null);
          });
        },
        function (cb) {
          (function waitFn(index) {
            if (index >= module.exports.queues.length) {
              return void cb(null);
            }
            waitConnected(module.exports.queues[index], (err) => {
              if (err) {
                return void cb(err);
              }
              waitFn(index + 1);
            });
          })(0);
        },
        function (cb) {
          const sendQueue = module.exports.queues[0];
          if (!sendQueue) {
            return void cb('should have send queue');
          }
          const handler = handlers[0];
          if (!handler) {
            return void cb('should have a handler');
          }

          const dataset = [Buffer.from('1'), Buffer.from('2')];
          (function sendFn(index) {
            if (index >= dataset.length) {
              return void cb(null);
            }
            sendQueue.sendMsg(dataset[index], (err) => {
              if (err) {
                return void cb(err);
              }
              sendFn(index + 1);
            });
          })(0);
        },
        function (cb) {
          const handler = handlers[0];
          (function waitFn(retry) {
            if (retry < 0) {
              return void cb(Error(`received ${handler.recvMessages.length}/2 messages`));
            } else if (handler.recvMessages.length === 2) {
              return void cb(null);
            }
            setTimeout(() => {
              waitFn(retry - 1);
            }, 10);
          })(150);
        },
        function (cb) {
          const handler = handlers[0];
          const msg1 = Buffer.from(handler.recvMessages[0]).toString();
          const msg2 = Buffer.from(handler.recvMessages[1]).toString();
          if (msg1 === msg2) {
            return void cb(Error('duplicate message'));
          }
          cb(null);
        },
      ],
      done
    );
  };
}

/**
 * Send unicast data to 3 receivers.
 *
 * @param {Engine} engine
 */
function dataUnicast1to3(engine) {
  return function (done) {
    let handlers;

    async.waterfall(
      [
        function (cb) {
          const opts = {
            name: 'name',
            isRecv: false,
            reliable: false,
            broadcast: false,
            prefetch: 1,
            sharedPrefix: '$share/general-mq/',
          };
          createMsgRsc(engine, opts, 3, (err, h) => {
            if (err) {
              return void cb(err);
            }
            handlers = h;
            cb(null);
          });
        },
        function (cb) {
          (function waitFn(index) {
            if (index >= module.exports.queues.length) {
              return void cb(null);
            }
            waitConnected(module.exports.queues[index], (err) => {
              if (err) {
                return void cb(err);
              }
              waitFn(index + 1);
            });
          })(0);
        },
        function (cb) {
          const sendQueue = module.exports.queues[0];
          if (!sendQueue) {
            return void cb('should have send queue');
          }
          const handler1 = handlers[0];
          if (!handler1) {
            return void cb('should have a handler 1');
          }
          const handler2 = handlers[0];
          if (!handler2) {
            return void cb('should have a handler 2');
          }
          const handler3 = handlers[0];
          if (!handler3) {
            return void cb('should have a handler 3');
          }

          const dataset = [
            Buffer.from('1'),
            Buffer.from('2'),
            Buffer.from('3'),
            Buffer.from('4'),
            Buffer.from('5'),
            Buffer.from('6'),
          ];
          (function sendFn(index) {
            if (index >= dataset.length) {
              return void cb(null);
            }
            sendQueue.sendMsg(dataset[index], (err) => {
              if (err) {
                return void cb(err);
              }
              sendFn(index + 1);
            });
          })(0);
        },
        function (cb) {
          const handler1 = handlers[0];
          const handler2 = handlers[1];
          const handler3 = handlers[2];
          (function waitFn(retry) {
            const len =
              handler1.recvMessages.length +
              handler2.recvMessages.length +
              handler3.recvMessages.length;
            if (retry < 0) {
              return void cb(Error(`received ${len}/6 messages`));
            } else if (len === 6) {
              return void cb(null);
            }
            setTimeout(() => {
              waitFn(retry - 1);
            }, 10);
          })(150);
        },
        function (cb) {
          const allMsg = [];
          const handler1 = handlers[0];
          const handler2 = handlers[1];
          const handler3 = handlers[2];
          for (const msg of handler1.recvMessages) {
            const s = Buffer.from(msg).toString();
            if (allMsg.includes(s)) {
              return void cb(Error('duplicate message'));
            }
            allMsg.push(s);
          }
          for (const msg of handler2.recvMessages) {
            const s = Buffer.from(msg).toString();
            if (allMsg.includes(s)) {
              return void cb(Error('duplicate message'));
            }
            allMsg.push(s);
          }
          for (const msg of handler3.recvMessages) {
            const s = Buffer.from(msg).toString();
            if (allMsg.includes(s)) {
              return void cb(Error('duplicate message'));
            }
            allMsg.push(s);
          }
          cb(null);
        },
      ],
      done
    );
  };
}

/**
 * Send broadcast data to one receiver.
 *
 * @param {Engine} engine
 */
function dataBroadcast1to1(engine) {
  return function (done) {
    let handlers;

    async.waterfall(
      [
        function (cb) {
          const opts = {
            name: 'name',
            isRecv: false,
            reliable: false,
            broadcast: true,
            prefetch: 1,
            sharedPrefix: '$share/general-mq/',
          };
          createMsgRsc(engine, opts, 1, (err, h) => {
            if (err) {
              return void cb(err);
            }
            handlers = h;
            cb(null);
          });
        },
        function (cb) {
          (function waitFn(index) {
            if (index >= module.exports.queues.length) {
              return void cb(null);
            }
            waitConnected(module.exports.queues[index], (err) => {
              if (err) {
                return void cb(err);
              }
              waitFn(index + 1);
            });
          })(0);
        },
        function (cb) {
          const sendQueue = module.exports.queues[0];
          if (!sendQueue) {
            return void cb('should have send queue');
          }
          const handler = handlers[0];
          if (!handler) {
            return void cb('should have a handler');
          }

          const dataset = [Buffer.from('1'), Buffer.from('2')];
          (function sendFn(index) {
            if (index >= dataset.length) {
              return void cb(null);
            }
            sendQueue.sendMsg(dataset[index], (err) => {
              if (err) {
                return void cb(err);
              }
              sendFn(index + 1);
            });
          })(0);
        },
        function (cb) {
          const handler = handlers[0];
          (function waitFn(retry) {
            if (retry < 0) {
              return void cb(Error(`received ${handler.recvMessages.length}/2 messages`));
            } else if (handler.recvMessages.length === 2) {
              return void cb(null);
            }
            setTimeout(() => {
              waitFn(retry - 1);
            }, 10);
          })(150);
        },
        function (cb) {
          const handler = handlers[0];
          const msg1 = Buffer.from(handler.recvMessages[0]).toString();
          const msg2 = Buffer.from(handler.recvMessages[1]).toString();
          if (msg1 === msg2) {
            return void cb(Error('duplicate message'));
          }
          cb(null);
        },
      ],
      done
    );
  };
}

/**
 * Send broadcast data to 3 receivers.
 *
 * @param {Engine} engine
 */
function dataBroadcast1to3(engine) {
  return function (done) {
    let handlers;

    async.waterfall(
      [
        function (cb) {
          const opts = {
            name: 'name',
            isRecv: false,
            reliable: false,
            broadcast: true,
            prefetch: 1,
            sharedPrefix: '$share/general-mq/',
          };
          createMsgRsc(engine, opts, 3, (err, h) => {
            if (err) {
              return void cb(err);
            }
            handlers = h;
            cb(null);
          });
        },
        function (cb) {
          (function waitFn(index) {
            if (index >= module.exports.queues.length) {
              return void cb(null);
            }
            waitConnected(module.exports.queues[index], (err) => {
              if (err) {
                return void cb(err);
              }
              waitFn(index + 1);
            });
          })(0);
        },
        function (cb) {
          const sendQueue = module.exports.queues[0];
          if (!sendQueue) {
            return void cb('should have send queue');
          }
          const handler1 = handlers[0];
          if (!handler1) {
            return void cb('should have a handler 1');
          }
          const handler2 = handlers[1];
          if (!handler2) {
            return void cb('should have a handler 2');
          }
          const handler3 = handlers[2];
          if (!handler3) {
            return void cb('should have a handler 3');
          }

          const dataset = [Buffer.from('1'), Buffer.from('2')];
          (function sendFn(index) {
            if (index >= dataset.length) {
              return void cb(null);
            }
            sendQueue.sendMsg(dataset[index], (err) => {
              if (err) {
                return void cb(err);
              }
              sendFn(index + 1);
            });
          })(0);
        },
        function (cb) {
          const handler1 = handlers[0];
          const handler2 = handlers[1];
          const handler3 = handlers[2];
          (function waitFn(retry) {
            const len1 = handler1.recvMessages.length;
            const len2 = handler2.recvMessages.length;
            const len3 = handler3.recvMessages.length;
            if (retry < 0) {
              return void cb(Error(`received ${len1 + len2 + len3}/6 messages`));
            } else if (len1 + len2 + len3 === 6) {
              if (len1 !== len2 || len2 != len3) {
                return void cb(Error('receive count not all 2'));
              }
              return void cb(null);
            }
            setTimeout(() => {
              waitFn(retry - 1);
            }, 10);
          })(150);
        },
        function (cb) {
          const handler1 = handlers[0];
          const handler2 = handlers[1];
          const handler3 = handlers[2];
          let msg1 = Buffer.from(handler1.recvMessages[0]).toString();
          let msg2 = Buffer.from(handler1.recvMessages[1]).toString();
          if (msg1 === msg2) {
            return void cb(Error('duplicate message handler 1'));
          }
          msg1 = Buffer.from(handler2.recvMessages[0]).toString();
          msg2 = Buffer.from(handler2.recvMessages[1]).toString();
          if (msg1 === msg2) {
            return void cb(Error('duplicate message handler 2'));
          }
          msg1 = Buffer.from(handler3.recvMessages[0]).toString();
          msg2 = Buffer.from(handler3.recvMessages[1]).toString();
          if (msg1 === msg2) {
            return void cb(Error('duplicate message handler 3'));
          }
          cb(null);
        },
      ],
      done
    );
  };
}

/**
 * Send reliable data by sending data to a closed queue then it will receive after connecting.
 *
 * @param {Engine} engine
 */
function dataReliable(engine) {
  return function (done) {
    let handlers;

    async.waterfall(
      [
        function (cb) {
          const opts = {
            name: 'name',
            isRecv: false,
            reliable: true,
            broadcast: false,
            prefetch: 1,
            sharedPrefix: '$share/general-mq/',
          };
          createMsgRsc(engine, opts, 1, (err, h) => {
            if (err) {
              return void cb(err);
            }
            handlers = h;
            cb(null);
          });
        },
        function (cb) {
          (function waitFn(index) {
            if (index >= module.exports.queues.length) {
              return void cb(null);
            }
            waitConnected(module.exports.queues[index], (err) => {
              if (err) {
                return void cb(err);
              }
              waitFn(index + 1);
            });
          })(0);
        },
        function (cb) {
          const sendQueue = module.exports.queues[0];
          if (!sendQueue) {
            return void cb('should have send queue');
          }
          const handler = handlers[0];
          if (!handler) {
            return void cb('should have a handler');
          }
          sendQueue.sendMsg(Buffer.from('1'), (err) => {
            if (err) {
              return cb(Error(`send 1 error: ${err}`));
            }

            (function waitFn(retry) {
              if (retry < 0) {
                return cb(Error('cannot receive 1'));
              } else if (handler.recvMessages.length !== 1) {
                return void setTimeout(() => {
                  waitFn(retry - 1);
                }, 10);
              }
              const msg = Buffer.from(handler.recvMessages[0]).toString();
              if (msg !== '1') {
                return cb(Error(`should receive 1, not ${msg}`));
              }
              cb(null);
            })(150);
          });
        },
        function (cb) {
          const queue = module.exports.queues[1];
          if (!queue) {
            return cb(Error('should have recv queue'));
          }
          queue.close((err) => {
            if (err) {
              return void cb(Error(`close recv error: ${err}`));
            }
            cb(null);
          });
        },
        function (cb) {
          const sendQueue = module.exports.queues[0];
          if (!sendQueue) {
            return void cb('should have send queue - 2');
          }
          const handler = handlers[0];
          if (!handler) {
            return void cb('should have a handler - 2');
          }
          sendQueue.sendMsg(Buffer.from('2'), (err) => {
            if (err) {
              return cb(Error(`send 2 error: ${err}`));
            }
            const queue = module.exports.queues[1];
            if (!queue) {
              return cb(Error('should have recv queue - 2'));
            }
            queue.connect();
            (function waitFn(retry) {
              if (retry < 0) {
                return cb(Error('cannot receive 2'));
              } else if (handler.recvMessages.length !== 2) {
                return void setTimeout(() => {
                  waitFn(retry - 1);
                }, 10);
              }
              const msg = Buffer.from(handler.recvMessages[1]).toString();
              if (msg !== '2') {
                return cb(Error(`should receive 2, not ${msg}`));
              }
              cb(null);
            })(150);
          });
        },
      ],
      done
    );
  };
}

/**
 * Send unreliable data by sending data to a closed queue then it SHOULD receive after connecting
 * because of AMQP.
 *
 * @param {Engine} engine
 */
function dataBestEffort(engine) {
  return function (done) {
    let handlers;

    async.waterfall(
      [
        function (cb) {
          const opts = {
            name: 'name',
            isRecv: false,
            reliable: false,
            broadcast: false,
            prefetch: 1,
            sharedPrefix: '$share/general-mq/',
          };
          createMsgRsc(engine, opts, 1, (err, h) => {
            if (err) {
              return void cb(err);
            }
            handlers = h;
            cb(null);
          });
        },
        function (cb) {
          (function waitFn(index) {
            if (index >= module.exports.queues.length) {
              return void cb(null);
            }
            waitConnected(module.exports.queues[index], (err) => {
              if (err) {
                return void cb(err);
              }
              waitFn(index + 1);
            });
          })(0);
        },
        function (cb) {
          const sendQueue = module.exports.queues[0];
          if (!sendQueue) {
            return void cb('should have send queue');
          }
          const handler = handlers[0];
          if (!handler) {
            return void cb('should have a handler');
          }
          sendQueue.sendMsg(Buffer.from('1'), (err) => {
            if (err) {
              return cb(Error(`send 1 error: ${err}`));
            }

            (function waitFn(retry) {
              if (retry < 0) {
                return cb(Error('cannot receive 1'));
              } else if (handler.recvMessages.length !== 1) {
                return void setTimeout(() => {
                  waitFn(retry - 1);
                }, 10);
              }
              const msg = Buffer.from(handler.recvMessages[0]).toString();
              if (msg !== '1') {
                return cb(Error(`should receive 1, not ${msg}`));
              }
              cb(null);
            })(150);
          });
        },
        function (cb) {
          const queue = module.exports.queues[1];
          if (!queue) {
            return cb(Error('should have recv queue'));
          }
          queue.close((err) => {
            if (err) {
              return void cb(Error(`close recv error: ${err}`));
            }
            cb(null);
          });
        },
        function (cb) {
          const sendQueue = module.exports.queues[0];
          if (!sendQueue) {
            return void cb('should have send queue - 2');
          }
          const handler = handlers[0];
          if (!handler) {
            return void cb('should have a handler - 2');
          }
          sendQueue.sendMsg(Buffer.from('2'), (err) => {
            if (err) {
              return cb(Error(`send 2 error: ${err}`));
            }
            const queue = module.exports.queues[1];
            if (!queue) {
              return cb(Error('should have recv queue - 2'));
            }
            queue.connect();
            (function waitFn(retry) {
              if (retry < 0) {
                return cb(null);
              } else if (handler.recvMessages.length !== 2) {
                return void setTimeout(() => {
                  waitFn(retry - 1);
                }, 10);
              }
              const msg = Buffer.from(handler.recvMessages[1]).toString();
              if (msg !== '2') {
                return cb(Error(`should receive 2, not ${msg}`));
              }
              cb(null);
            })(150);
          });
        },
      ],
      done
    );
  };
}

/**
 * Send persistent data by sending data to a closed queue then it will receive after connecting.
 *
 * @param {Engine} engine
 */
function dataPersistent(engine) {
  return function (done) {
    let handlers;

    async.waterfall(
      [
        function (cb) {
          const opts = {
            name: 'name',
            isRecv: false,
            reliable: true,
            broadcast: false,
            prefetch: 1,
            persistent: true,
            sharedPrefix: '$share/general-mq/',
          };
          createMsgRsc(engine, opts, 1, (err, h) => {
            if (err) {
              return void cb(err);
            }
            handlers = h;
            cb(null);
          });
        },
        function (cb) {
          (function waitFn(index) {
            if (index >= module.exports.queues.length) {
              return void cb(null);
            }
            waitConnected(module.exports.queues[index], (err) => {
              if (err) {
                return void cb(err);
              }
              waitFn(index + 1);
            });
          })(0);
        },
        function (cb) {
          const sendQueue = module.exports.queues[0];
          if (!sendQueue) {
            return void cb('should have send queue');
          }
          const handler = handlers[0];
          if (!handler) {
            return void cb('should have a handler');
          }
          sendQueue.sendMsg(Buffer.from('1'), (err) => {
            if (err) {
              return cb(Error(`send 1 error: ${err}`));
            }

            (function waitFn(retry) {
              if (retry < 0) {
                return cb(Error('cannot receive 1'));
              } else if (handler.recvMessages.length !== 1) {
                return void setTimeout(() => {
                  waitFn(retry - 1);
                }, 10);
              }
              const msg = Buffer.from(handler.recvMessages[0]).toString();
              if (msg !== '1') {
                return cb(Error(`should receive 1, not ${msg}`));
              }
              cb(null);
            })(150);
          });
        },
        function (cb) {
          const queue = module.exports.queues[1];
          if (!queue) {
            return cb(Error('should have recv queue'));
          }
          queue.close((err) => {
            if (err) {
              return void cb(Error(`close recv error: ${err}`));
            }
            cb(null);
          });
        },
        function (cb) {
          const sendQueue = module.exports.queues[0];
          if (!sendQueue) {
            return void cb('should have send queue - 2');
          }
          const handler = handlers[0];
          if (!handler) {
            return void cb('should have a handler - 2');
          }
          sendQueue.sendMsg(Buffer.from('2'), (err) => {
            if (err) {
              return cb(Error(`send 2 error: ${err}`));
            }
            const queue = module.exports.queues[1];
            if (!queue) {
              return cb(Error('should have recv queue - 2'));
            }
            queue.connect();
            (function waitFn(retry) {
              if (retry < 0) {
                return cb(Error('cannot receive 2'));
              } else if (handler.recvMessages.length !== 2) {
                return void setTimeout(() => {
                  waitFn(retry - 1);
                }, 10);
              }
              const msg = Buffer.from(handler.recvMessages[1]).toString();
              if (msg !== '2') {
                return cb(Error(`should receive 2, not ${msg}`));
              }
              cb(null);
            })(150);
          });
        },
      ],
      done
    );
  };
}

/**
 * Test NACK and then the queue will receive the data again.
 *
 * @param {Engine} engine
 */
function dataNack(engine) {
  return function (done) {
    let handlers;

    async.waterfall(
      [
        function (cb) {
          const opts = {
            name: 'name',
            isRecv: false,
            reliable: true,
            broadcast: false,
            prefetch: 1,
            sharedPrefix: '$share/general-mq/',
          };
          createMsgRsc(engine, opts, 1, (err, h) => {
            if (err) {
              return void cb(err);
            }
            handlers = h;
            cb(null);
          });
        },
        function (cb) {
          (function waitFn(index) {
            if (index >= module.exports.queues.length) {
              return void cb(null);
            }
            waitConnected(module.exports.queues[index], (err) => {
              if (err) {
                return void cb(err);
              }
              waitFn(index + 1);
            });
          })(0);
        },
        function (cb) {
          const sendQueue = module.exports.queues[0];
          if (!sendQueue) {
            return void cb('should have send queue');
          }
          const handler = handlers[0];
          if (!handler) {
            return void cb('should have a handler');
          }
          handler.useNack = true;
          sendQueue.sendMsg(Buffer.from('1'), (err) => {
            if (err) {
              return cb(Error(`send 1 error: ${err}`));
            }

            (function waitFn(retry) {
              if (retry < 0) {
                return cb(Error('cannot receive 1 for nack'));
              } else if (handler.nackMessages.length === 0) {
                return void setTimeout(() => {
                  waitFn(retry - 1);
                }, 10);
              }
              const msg = Buffer.from(handler.nackMessages[0]).toString();
              if (msg !== '1') {
                return cb(Error(`should receive 1, not ${msg}`));
              }
              handler.useNack = false;
              cb(null);
            })(150);
          });
        },
        function (cb) {
          const handler = handlers[0];
          (function waitFn(retry) {
            if (retry < 0) {
              return cb(Error('cannot receive 1'));
            } else if (handler.recvMessages.length !== 1) {
              return void setTimeout(() => {
                waitFn(retry - 1);
              }, 10);
            }
            const msg = Buffer.from(handler.recvMessages[0]).toString();
            if (msg !== '1') {
              return cb(Error(`should receive 1, not ${msg}`));
            }
            cb(null);
          })(150);
        },
      ],
      done
    );
  };
}

/**
 * Test ACK/NACK with wrong parameters.
 *
 * @param {Engine} engine
 */
function dataAckNackWrong(engine) {
  return function () {
    const conn = new engine.Connection();
    assert.ok(conn);
    const opts = {
      name: 'name',
      isRecv: false,
      reliable: false,
      broadcast: false,
    };
    const queue = new engine.Queue(opts, conn);
    assert.ok(queue);

    assert.throws(() => {
      queue.ack(null, () => {});
    });
    assert.throws(() => {
      queue.ack({}, {});
    });
    assert.throws(() => {
      queue.nack(null, () => {});
    });
    assert.throws(() => {
      queue.nack({}, {});
    });
  };
}

/**
 * @param {AmqpQueue|MqttQueue} queue
 * @param {function} callback
 *   @param {?Error} callback.err
 */
function waitConnected(queue, callback) {
  let retry = RETRY_10MS;
  const waitFn = function () {
    if (retry < 0) {
      return void callback(Error('not connected'));
    } else if (queue.status() === Status.Connected) {
      return void callback(null);
    }
    retry--;
    setTimeout(waitFn, 10);
  };
  setTimeout(waitFn, 10);
}

/**
 * Create connected (optional) connections/queues for testing connections.
 *
 * @param {Engine} engine
 * @param {Object} handlers
 *   @param {function} [handlers.error] Errors handler.
 *     @param {Errors} handlers.errors.err
 *   @param {function} [handlers.status] Status handler.
 *     @param {Status} handlers.status.status
 *   @param {AmqpQueueMsgHandler|MqttQueueMsgHandler} [handlers.msg] Message handler.
 * @param {boolean} connect To connect the queue.
 * @param {function} callback
 *   @param {?Error} callback.err
 */
function createConnRsc(engine, handlers, connect, callback) {
  const conn = new engine.Connection();
  assert.ok(conn);
  module.exports.conn.push(conn);

  const opts = {
    name: 'name',
    isRecv: false,
    reliable: false,
    broadcast: false,
  };
  const queue = new engine.Queue(opts, conn);
  assert.ok(queue);
  module.exports.queues.push(queue);

  if (handlers.error) {
    queue.on(Events.Error, handlers.error);
  }
  if (handlers.status) {
    queue.on(Events.Status, handlers.status);
  }
  if (handlers.msg) {
    assert.doesNotThrow(() => {
      queue.setMsgHandler(handlers.msg);
    });
  }

  if (!connect) {
    return void process.nextTick(() => {
      callback(null);
    });
  }

  conn.connect();
  queue.connect();
  process.nextTick(() => {
    callback(null);
  });
}

/**
 * Create connected (optional) connections/queues for testing messages.
 *
 * @param {Engine} engine
 * @param {Object} opts The queue options. Refer to AMQP/MQTT queues.
 *   @param {string} opts.name
 *   @param {boolean} opts.isRecv
 *   @param {boolean} opts.reliable
 *   @param {boolean} opts.broadcast
 *   @param {number} [opts.reconnectMillis=1000]
 *   @param {number} [opts.prefetch]
 *   @param {boolean} [opts.persistent]
 *   @param {string} [opts.sharedPrefix]
 * @param {number} receiverCount Number of receivers to receive messages from the queue.
 * @param {function} callback
 *   @param {?Error} callback.err
 *   @param {?[]TestRecvMsgHandler} callback.handlers
 */
function createMsgRsc(engine, opts, receiverCount, callback) {
  const conn = new engine.Connection();
  assert.ok(conn);
  module.exports.conn.push(conn);

  const retHandlers = [];

  const sendOpts = { ...opts };
  sendOpts.isRecv = false;
  const queue = new engine.Queue(sendOpts, conn);
  assert.ok(queue);
  module.exports.queues.push(queue);

  for (let i = 0; i < receiverCount; i++) {
    let innerConn = conn;
    if (engine === gmq.mqtt) {
      const conn = new engine.Connection();
      assert.ok(conn);
      module.exports.conn.push(conn);
      innerConn = conn;
    }
    const recvOpts = { ...opts };
    recvOpts.isRecv = true;
    const queue = new engine.Queue(recvOpts, innerConn);
    assert.ok(queue);
    const handler = new TestRecvMsgHandler();
    queue.setMsgHandler(handler.onMessage.bind(handler));
    module.exports.queues.push(queue);
    retHandlers.push(handler);
  }

  for (let c of module.exports.conn) {
    assert.doesNotThrow(() => {
      c.connect();
    });
  }
  for (let q of module.exports.queues) {
    assert.doesNotThrow(() => {
      q.connect();
    });
  }

  process.nextTick(() => {
    callback(null, retHandlers);
  });
}

module.exports = {
  conn: [],
  queues: [],

  afterEach,
  newDefault,
  newZero,
  newWrongOpts,
  properties,
  connectNoHandler,
  connectWithHandler,
  connectAfterConnect,
  setMsgHandler,
  close,
  closeAfterClose,
  sendError,
  reconnect,
  dataUnicast1to1,
  dataUnicast1to3,
  dataBroadcast1to1,
  dataBroadcast1to3,
  dataReliable,
  dataBestEffort,
  dataPersistent,
  dataNack,
  dataAckNackWrong,
};
