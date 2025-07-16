'use strict';

const assert = require('assert');

const gmq = require('..');
const { AmqpConnection, AmqpQueue, MqttConnection, MqttQueue, SdkError } = gmq;
const { Errors, Events, Status } = require('../lib/constants');

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
    if (this.useNack) {
      queue
        .nack(msg)
        .then(() => this.nackMessages.push(msg.payload))
        .catch((err) => this.nackErrors.push(err));
    } else {
      queue
        .ack(msg)
        .then(() => this.recvMessages.push(msg.payload))
        .catch((err) => this.ackErrors.push(err));
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

async function afterEach() {
  let withErr = null;
  for (;;) {
    const queue = module.exports.queues.pop();
    if (!queue) {
      break;
    }
    await queue.close().catch((err) => (withErr = err));
  }
  for (;;) {
    const conn = module.exports.conn.pop();
    if (!conn) {
      assert.ok(!withErr, withErr);
      return;
    }
    await conn.close().catch((err) => (withErr = err));
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

    assert.throws(() => new engine.Queue(null, conn));
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
  return async function () {
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
    assert.doesNotThrow(() => queue.setMsgHandler((_queue, _msg) => {}));

    module.exports.conn.push(conn);
    module.exports.queues.push(queue);
    conn.connect();
    queue.connect();
    await waitConnected(queue);
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

    assert.doesNotThrow(() => queue.setMsgHandler((_msg) => {}));
  };
}

/**
 * Test `close()`.
 *
 * @param {Engine} engine
 */
function close(engine) {
  return async function () {
    let recvClosed = false;
    const handlers = {
      status: function (status) {
        if (status === Status.Closed) {
          recvClosed = true;
        }
      },
    };
    await createConnRsc(engine, handlers, true);
    for (const q of module.exports.queues) {
      await waitConnected(q);
    }
    const q = module.exports.queues[0];
    await q.close();
    assert.ok(recvClosed);
  };
}

/**
 * Test `close()` for a closed queue.
 *
 * @param {Engine} engine
 */
function closeAfterClose(engine) {
  return async function () {
    await createConnRsc(engine, {}, true);
    for (const q of module.exports.queues) {
      await waitConnected(q);
    }
    const q = module.exports.queues[0];
    await q.close();
    await q.close();
  };
}

/**
 * Test send with an invalid queue.
 *
 * @param {Engine} engine
 */
function sendError(engine) {
  return async function () {
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

    const shouldErrFn = () => {
      throw Error('should error');
    };
    const catchFn = (err) => assert.ok(!(err instanceof SdkError));

    assert.throws(() => queue.connect());
    assert.throws(() => queue.setMsgHandler({}));
    assert.doesNotThrow(() => queue.setMsgHandler((_queue, _msg) => {}));
    queue.sendMsg('payload').then(shouldErrFn).catch(catchFn);
    queue.sendMsg(Buffer.from(''), {}).then(shouldErrFn).catch(catchFn);
    queue
      .sendMsg(Buffer.alloc(0))
      .then(() => {
        throw Error('send to not-connected queue should error');
      })
      .catch((_err) => {});

    module.exports.conn.push(conn);
    module.exports.queues.push(queue);
    conn.connect();
    queue.connect();
    await waitConnected(queue);
    queue
      .sendMsg(Buffer.alloc(0))
      .then(() => {
        throw Error('send to recv queue should error');
      })
      .catch((_err) => {});
  };
}

/**
 * Test reconnect by closing/connecting the associated connection.
 *
 * @param {Engine} engine
 */
function reconnect(engine) {
  return async function () {
    let recvConnecting = false;
    const handlers = {
      status: function (status) {
        if (status === Status.Connecting) {
          recvConnecting = true;
        }
      },
    };
    await createConnRsc(engine, handlers, true);
    for (const q of module.exports.queues) {
      await waitConnected(q);
    }

    const conn = module.exports.conn[0];
    assert.ok(conn, 'should have a connection');
    const queue = module.exports.queues[0];
    assert.ok(queue, 'should have a queue');

    recvConnecting = false;
    await conn.close();
    for (let i = 0; i < 200; i++) {
      if (recvConnecting) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.ok(recvConnecting);

    conn.connect();
    await waitConnected(queue);
  };
}

/**
 * Send unicast data to one receiver.
 *
 * @param {Engine} engine
 */
function dataUnicast1to1(engine) {
  return async function () {
    const opts = {
      name: 'name',
      isRecv: false,
      reliable: false,
      broadcast: false,
      prefetch: 1,
      sharedPrefix: '$share/general-mq/',
    };
    const handlers = await createMsgRsc(engine, opts, 1);
    for (const q of module.exports.queues) {
      await waitConnected(q);
    }
    const sendQueue = module.exports.queues[0];
    assert.ok(sendQueue, 'should have send queue');
    const handler = handlers[0];
    assert.ok(handler, 'should have a handler');

    const dataset = [Buffer.from('1'), Buffer.from('2')];
    for (const data of dataset) {
      await sendQueue.sendMsg(data);
    }

    let len;
    for (let i = 0; i < 150; i++) {
      len = handler.recvMessages.length;
      if (len === 2) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(len, 2, `received ${len}/2 messages`);

    const msg1 = Buffer.from(handler.recvMessages[0]).toString();
    const msg2 = Buffer.from(handler.recvMessages[1]).toString();
    assert.notStrictEqual(msg1, msg2, 'duplicate message');
  };
}

/**
 * Send unicast data to 3 receivers.
 *
 * @param {Engine} engine
 */
function dataUnicast1to3(engine) {
  return async function () {
    const opts = {
      name: 'name',
      isRecv: false,
      reliable: false,
      broadcast: false,
      prefetch: 1,
      sharedPrefix: '$share/general-mq/',
    };
    const handlers = await createMsgRsc(engine, opts, 3);
    for (const q of module.exports.queues) {
      await waitConnected(q);
    }
    const sendQueue = module.exports.queues[0];
    assert.ok(sendQueue, 'should have send queue');
    const handler1 = handlers[0];
    assert.ok(handler1, 'should have a handler 1');
    const handler2 = handlers[1];
    assert.ok(handler2, 'should have a handler 2');
    const handler3 = handlers[2];
    assert.ok(handler3, 'should have a handler 3');

    const dataset = [
      Buffer.from('1'),
      Buffer.from('2'),
      Buffer.from('3'),
      Buffer.from('4'),
      Buffer.from('5'),
      Buffer.from('6'),
    ];
    for (const data of dataset) {
      await sendQueue.sendMsg(data);
    }

    let len;
    for (let i = 0; i < 150; i++) {
      len =
        handler1.recvMessages.length + handler2.recvMessages.length + handler3.recvMessages.length;
      if (len === 6) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(len, 6, `received ${len}/6 messages`);

    const allMsg = [];
    for (const msg of handler1.recvMessages) {
      const s = Buffer.from(msg).toString();
      assert.ok(!allMsg.includes(s), 'duplicate message');
      allMsg.push(s);
    }
    for (const msg of handler2.recvMessages) {
      const s = Buffer.from(msg).toString();
      assert.ok(!allMsg.includes(s), 'duplicate message');
      allMsg.push(s);
    }
    for (const msg of handler3.recvMessages) {
      const s = Buffer.from(msg).toString();
      assert.ok(!allMsg.includes(s), 'duplicate message');
      allMsg.push(s);
    }
  };
}

/**
 * Send broadcast data to one receiver.
 *
 * @param {Engine} engine
 */
function dataBroadcast1to1(engine) {
  return async function () {
    const opts = {
      name: 'name',
      isRecv: false,
      reliable: false,
      broadcast: true,
      prefetch: 1,
      sharedPrefix: '$share/general-mq/',
    };
    const handlers = await createMsgRsc(engine, opts, 1);
    for (const q of module.exports.queues) {
      await waitConnected(q);
    }
    const sendQueue = module.exports.queues[0];
    assert.ok(sendQueue, 'should have send queue');
    const handler = handlers[0];
    assert.ok(handler, 'should have a handler');

    const dataset = [Buffer.from('1'), Buffer.from('2')];
    for (const data of dataset) {
      await sendQueue.sendMsg(data);
    }

    let len;
    for (let i = 0; i < 150; i++) {
      len = handler.recvMessages.length;
      if (len === 2) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(len, 2, `received ${len}/2 messages`);

    const msg1 = Buffer.from(handler.recvMessages[0]).toString();
    const msg2 = Buffer.from(handler.recvMessages[1]).toString();
    assert.notStrictEqual(msg1, msg2, 'duplicate message');
  };
}

/**
 * Send broadcast data to 3 receivers.
 *
 * @param {Engine} engine
 */
function dataBroadcast1to3(engine) {
  return async function () {
    const opts = {
      name: 'name',
      isRecv: false,
      reliable: false,
      broadcast: true,
      prefetch: 1,
      sharedPrefix: '$share/general-mq/',
    };
    const handlers = await createMsgRsc(engine, opts, 3);
    for (const q of module.exports.queues) {
      await waitConnected(q);
    }
    const sendQueue = module.exports.queues[0];
    assert.ok(sendQueue, 'should have send queue');
    const handler1 = handlers[0];
    assert.ok(handler1, 'should have a handler 1');
    const handler2 = handlers[1];
    assert.ok(handler2, 'should have a handler 2');
    const handler3 = handlers[2];
    assert.ok(handler3, 'should have a handler 3');

    const dataset = [Buffer.from('1'), Buffer.from('2')];
    for (const data of dataset) {
      await sendQueue.sendMsg(data);
    }

    let len1, len2, len3;
    for (let i = 0; i < 150; i++) {
      len1 = handler1.recvMessages.length;
      len2 = handler2.recvMessages.length;
      len3 = handler3.recvMessages.length;
      if (len1 + len2 + len3 === 6) {
        assert.ok(len1 === len2 && len2 === len3, 'receive count not all 2');
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(len1 + len2 + len3, 6, `received ${len1 + len2 + len3}/6 messages`);

    let msg1 = Buffer.from(handler1.recvMessages[0]).toString();
    let msg2 = Buffer.from(handler1.recvMessages[1]).toString();
    assert.notStrictEqual(msg1, msg2, 'duplicate message handler 1');
    msg1 = Buffer.from(handler2.recvMessages[0]).toString();
    msg2 = Buffer.from(handler2.recvMessages[1]).toString();
    assert.notStrictEqual(msg1, msg2, 'duplicate message handler 2');
    msg1 = Buffer.from(handler3.recvMessages[0]).toString();
    msg2 = Buffer.from(handler3.recvMessages[1]).toString();
    assert.notStrictEqual(msg1, msg2, 'duplicate message handler 3');
  };
}

/**
 * Send reliable data by sending data to a closed queue then it will receive after connecting.
 *
 * @param {Engine} engine
 */
function dataReliable(engine) {
  return async function () {
    const opts = {
      name: 'name',
      isRecv: false,
      reliable: true,
      broadcast: false,
      prefetch: 1,
      sharedPrefix: '$share/general-mq/',
    };
    const handlers = await createMsgRsc(engine, opts, 1);
    for (const q of module.exports.queues) {
      await waitConnected(q);
    }
    const sendQueue = module.exports.queues[0];
    assert.ok(sendQueue, 'should have send queue');
    const handler = handlers[0];
    assert.ok(handler, 'should have a handler');
    const queue = module.exports.queues[1];
    assert.ok(queue, 'should have recv queue');

    await sendQueue.sendMsg(Buffer.from('1'));
    let len;
    for (let i = 0; i < 150; i++) {
      len = handler.recvMessages.length;
      if (len === 1) {
        const msg = Buffer.from(handler.recvMessages[0]).toString();
        assert.strictEqual(msg, '1', `should receive 1, not ${msg}`);
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(len, 1, 'cannot receive 1');

    await queue.close();
    await sendQueue.sendMsg(Buffer.from('2'));
    queue.connect();
    for (let i = 0; i < 150; i++) {
      len = handler.recvMessages.length;
      if (len === 2) {
        const msg = Buffer.from(handler.recvMessages[1]).toString();
        assert.strictEqual(msg, '2', `should receive 2, not ${msg}`);
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(len, 2, 'cannot receive 2');
  };
}

/**
 * Send unreliable data by sending data to a closed queue then it SHOULD receive after connecting
 * because of AMQP.
 *
 * @param {Engine} engine
 */
function dataBestEffort(engine) {
  return async function () {
    const opts = {
      name: 'name',
      isRecv: false,
      reliable: false,
      broadcast: false,
      prefetch: 1,
      sharedPrefix: '$share/general-mq/',
    };
    const handlers = await createMsgRsc(engine, opts, 1);
    for (const q of module.exports.queues) {
      await waitConnected(q);
    }
    const sendQueue = module.exports.queues[0];
    assert.ok(sendQueue, 'should have send queue');
    const handler = handlers[0];
    assert.ok(handler, 'should have a handler');
    const queue = module.exports.queues[1];
    assert.ok(queue, 'should have recv queue');

    await sendQueue.sendMsg(Buffer.from('1'));
    let len;
    for (let i = 0; i < 150; i++) {
      len = handler.recvMessages.length;
      if (len === 1) {
        const msg = Buffer.from(handler.recvMessages[0]).toString();
        assert.strictEqual(msg, '1', `should receive 1, not ${msg}`);
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(len, 1, 'cannot receive 1');

    await queue.close();
    await sendQueue.sendMsg(Buffer.from('2'));
    queue.connect();
    for (let i = 0; i < 150; i++) {
      len = handler.recvMessages.length;
      if (len === 2) {
        const msg = Buffer.from(handler.recvMessages[1]).toString();
        assert.strictEqual(msg, '2', `should receive 2, not ${msg}`);
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(len, 2, 'cannot receive 2');
  };
}

/**
 * Send persistent data by sending data to a closed queue then it will receive after connecting.
 *
 * @param {Engine} engine
 */
function dataPersistent(engine) {
  return async function () {
    const opts = {
      name: 'name',
      isRecv: false,
      reliable: true,
      broadcast: false,
      prefetch: 1,
      persistent: true,
      sharedPrefix: '$share/general-mq/',
    };
    const handlers = await createMsgRsc(engine, opts, 1);
    for (const q of module.exports.queues) {
      await waitConnected(q);
    }
    const sendQueue = module.exports.queues[0];
    assert.ok(sendQueue, 'should have send queue');
    const handler = handlers[0];
    assert.ok(handler, 'should have a handler');
    const queue = module.exports.queues[1];
    assert.ok(queue, 'should have recv queue');

    await sendQueue.sendMsg(Buffer.from('1'));
    let len;
    for (let i = 0; i < 150; i++) {
      len = handler.recvMessages.length;
      if (len === 1) {
        const msg = Buffer.from(handler.recvMessages[0]).toString();
        assert.strictEqual(msg, '1', `should receive 1, not ${msg}`);
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(len, 1, 'cannot receive 1');

    await queue.close();
    await sendQueue.sendMsg(Buffer.from('2'));
    queue.connect();
    for (let i = 0; i < 150; i++) {
      len = handler.recvMessages.length;
      if (len === 2) {
        const msg = Buffer.from(handler.recvMessages[1]).toString();
        assert.strictEqual(msg, '2', `should receive 2, not ${msg}`);
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(len, 2, 'cannot receive 2');
  };
}

/**
 * Test NACK and then the queue will receive the data again.
 *
 * @param {Engine} engine
 */
function dataNack(engine) {
  return async function () {
    const opts = {
      name: 'name',
      isRecv: false,
      reliable: true,
      broadcast: false,
      prefetch: 1,
      sharedPrefix: '$share/general-mq/',
    };
    const handlers = await createMsgRsc(engine, opts, 1);
    for (const q of module.exports.queues) {
      await waitConnected(q);
    }
    const sendQueue = module.exports.queues[0];
    assert.ok(sendQueue, 'should have send queue');
    const handler = handlers[0];
    assert.ok(handler, 'should have a handler');
    handler.useNack = true;
    await sendQueue.sendMsg(Buffer.from('1'));
    let len;
    for (let i = 0; i < 150; i++) {
      len = handler.nackMessages.length;
      if (len > 0) {
        const msg = Buffer.from(handler.nackMessages[0]).toString();
        assert.strictEqual(msg, '1', `should receive 1, not ${msg}`);
        handler.useNack = false;
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.notStrictEqual(len, 0, 'cannot receive 1 for nack');
    for (let i = 0; i < 150; i++) {
      len = handler.recvMessages.length;
      if (len === 1) {
        const msg = Buffer.from(handler.recvMessages[0]).toString();
        assert.strictEqual(msg, '1', `should receive 1, not ${msg}`);
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(len, 1, 'cannot receive 1');
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

    const shouldErrFn = () => {
      throw Error('should error');
    };
    const catchFn = (err) => assert.ok(!(err instanceof SdkError));

    queue
      .ack(null, () => {})
      .then(shouldErrFn)
      .catch(catchFn);
    queue.ack({}, {}).then(shouldErrFn).catch(catchFn);
    queue
      .nack(null, () => {})
      .then(shouldErrFn)
      .catch(catchFn);
    queue.nack({}, {}).then(shouldErrFn).catch(catchFn);
  };
}

/**
 * @private
 * @async
 * @param {AmqpQueue|MqttQueue} queue
 */
async function waitConnected(queue) {
  for (let i = 0; i < RETRY_10MS; i++) {
    await new Promise((resolve) => setTimeout(resolve, 10));
    if (queue.status() === Status.Connected) {
      return;
    }
  }
  throw Error('not connected');
}

/**
 * Create connected (optional) connections/queues for testing connections.
 *
 * @private
 * @async
 * @param {Engine} engine
 * @param {Object} handlers
 *   @param {function} [handlers.error] Errors handler.
 *     @param {Errors} handlers.errors.err
 *   @param {function} [handlers.status] Status handler.
 *     @param {Status} handlers.status.status
 *   @param {AmqpQueueMsgHandler|MqttQueueMsgHandler} [handlers.msg] Message handler.
 * @param {boolean} connect To connect the queue.
 */
async function createConnRsc(engine, handlers, connect) {
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
    assert.doesNotThrow(() => queue.setMsgHandler(handlers.msg));
  }

  if (!connect) {
    return;
  }

  conn.connect();
  queue.connect();
}

/**
 * Create connected (optional) connections/queues for testing messages.
 *
 * @private
 * @async
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
 * @returns {Promise<[]TestRecvMsgHandler>} Receive message handlers.
 */
async function createMsgRsc(engine, opts, receiverCount) {
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
    assert.doesNotThrow(() => c.connect());
  }
  for (let q of module.exports.queues) {
    assert.doesNotThrow(() => q.connect());
  }

  return retHandlers;
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
