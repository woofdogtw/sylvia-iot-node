'use strict';

const { Agent } = require('http');
const { URL } = require('url');

const superagent = require('superagent');

const gmq = require('general-mq');
const { AmqpQueue, MqttQueue } = gmq;
const { Events, Status } = gmq.constants;

const { ApplicationMgr } = require('../../mq/application');
const { Connection } = require('../../mq/lib');
const { NetworkMgr } = require('../../mq/network');

/**
 * Connection with counter.
 *
 * @typedef {Object} CounterConnection
 * @property {gmq.AmqpConnection|gmq.MqttConnection} conn
 * @property {number} count
 */

const keepAliveAgent = new Agent({ keepAlive: true });

async function afterEachFn() {
  for (;;) {
    const mgr = module.exports.appMgrs.pop();
    if (!mgr) {
      break;
    }
    mgr.removeAllListeners();
    await mgr.close();
  }
  for (;;) {
    const mgr = module.exports.netMgrs.pop();
    if (!mgr) {
      break;
    }
    mgr.removeAllListeners();
    await mgr.close();
  }
  for (const [_, conn] of module.exports.mgrConns) {
    conn.conn.removeAllListeners();
    await conn.conn.close();
  }
  module.exports.mgrConns.clear();
  for (;;) {
    const queue = module.exports.appNetQueues.pop();
    if (!queue) {
      break;
    }
    queue.removeAllListeners();
    await queue.close();
  }
  const conn = module.exports.appNetConn;
  module.exports.appNetConn = null;
  if (conn) {
    conn.conn.removeAllListeners();
    await conn.conn.close();
  }
}

/**
 * @async
 * @param {gmq.Engine} engine
 * @returns {Promise<CounterConnection>}
 */
async function newConnection(engine) {
  return new Promise((resolve) => {
    const conn = new engine.Connection();
    conn.on(Events.Status, (status) => {
      if (status === Status.Connected) {
        return void resolve({
          conn,
          counter: 0,
        });
      }
    });
    conn.connect();
  });
}

function connHostUri(engine) {
  if (engine === gmq.amqp) {
    return new URL('amqp://localhost');
  } else if (engine === gmq.mqtt) {
    return new URL('mqtt://localhost');
  }
  throw Error('unsupport engine');
}

async function removeRabbitmqQueues() {
  let res = await superagent
    .agent(keepAliveAgent)
    .auth('guest', 'guest')
    .get('http://localhost:15672/api/queues/%2f')
    .catch((err) => {
      throw Error(`get queue error: ${err}`);
    });
  if (res.statusCode !== 200) {
    throw Error(`get queues with status ${res.statusCode}`);
  }
  const queues = res.body;
  for (;;) {
    const queue = queues.pop();
    if (!queue) {
      break;
    } else if (queue.name.startsWith('amq.')) {
      continue;
    }

    res = await superagent
      .agent(keepAliveAgent)
      .auth('guest', 'guest')
      .delete(`http://localhost:15672/api/queues/%2f/${queue.name}`)
      .catch((err) => {
        throw Error(`delete queue ${queue.name} error: ${err}`);
      });
    if (res.statusCode !== 204 && res.statusCode !== 404) {
      throw Error(`delete queues with status ${res.statusCode}`);
    }
  }
}

module.exports = {
  /** @type {Map<string, Connection>} */
  mgrConns: new Map(),
  /** @type {ApplicationMgr[]} */
  appMgrs: [],
  /** @type {NetworkMgr[]} */
  netMgrs: [],
  /** @type {Connection|null} */
  appNetConn: null,
  /** @type {AmqpQueue[]|MqttQueue[]} */
  appNetQueues: [],

  SHARED_PREFIX: '$share/sylvia-iot-sdk/',

  afterEachFn,
  newConnection,
  connHostUri,
  removeRabbitmqQueues,
};
