'use strict';

const { Agent } = require('http');
const { URL } = require('url');

const async = require('async');
const superagent = require('superagent');

const gmq = require('general-mq');
const { AmqpQueue } = require('general-mq/lib/amqp-queue');
const { Events, Status } = require('general-mq/lib/constants');
const { MqttQueue } = require('general-mq/lib/mqtt-queue');

const { ApplicationMgr } = require('../../mq/application');
const { Connection } = require('../../mq/lib');
const { NetworkMgr } = require('../../mq/network');

const keepAliveAgent = new Agent({ keepAlive: true });

function afterEachFn(done) {
  async.waterfall(
    [
      function (cb) {
        (function waitFn() {
          const mgr = module.exports.appMgrs.pop();
          if (!mgr) {
            return void cb(null);
          }
          mgr.removeAllListeners();
          mgr.close((err) => {
            if (err) {
              return void cb(err);
            }
            waitFn();
          });
        })();
      },
      function (cb) {
        (function waitFn() {
          const mgr = module.exports.netMgrs.pop();
          if (!mgr) {
            return void cb(null);
          }
          mgr.removeAllListeners();
          mgr.close((err) => {
            if (err) {
              return void cb(err);
            }
            waitFn();
          });
        })();
      },
      function (cb) {
        const keys = [];
        for (const k of module.exports.mgrConns.keys()) {
          keys.push(k);
        }
        (function waitFn(index) {
          if (index >= keys.length) {
            module.exports.mgrConns.clear();
            return void cb(null);
          }
          const conn = module.exports.mgrConns.get(keys[index]);
          conn.conn.removeAllListeners();
          conn.conn.close((err) => {
            if (err) {
              return void cb(err);
            }
            waitFn(index + 1);
          });
        })(0);
      },
      function (cb) {
        (function waitFn() {
          const queue = module.exports.appNetQueues.pop();
          if (!queue) {
            return void cb(null);
          }
          queue.removeAllListeners();
          queue.close((err) => {
            if (err) {
              return void cb(err);
            }
            waitFn();
          });
        })();
      },
      function (cb) {
        const conn = module.exports.appNetConn;
        module.exports.appNetConn = null;
        if (!conn) {
          return void cb(null);
        }
        conn.conn.removeAllListeners();
        conn.conn.close((err) => {
          cb(err || null);
        });
      },
    ],
    done
  );
}

function newConnection(engine, callback) {
  const conn = new engine.Connection();
  conn.on(Events.Status, (status) => {
    if (status === Status.Connected) {
      callback(null, {
        conn,
        counter: 0,
      });
    }
  });
  conn.connect();
}

function connHostUri(engine) {
  if (engine === gmq.amqp) {
    return new URL('amqp://localhost');
  } else if (engine === gmq.mqtt) {
    return new URL('mqtt://localhost');
  }
  throw Error('unsupport engine');
}

function removeRabbitmqQueues(done) {
  function removeQueues(queues, callback) {
    const queue = queues.pop();
    if (!queue) {
      return void process.nextTick(() => {
        callback(null);
      });
    } else if (queue.name.startsWith('amq.')) {
      return void process.nextTick(() => {
        removeQueues(queues, callback);
      });
    }

    superagent
      .agent(keepAliveAgent)
      .auth('guest', 'guest')
      .delete(`http://localhost:15672/api/queues/%2f/${queue.name}`, (err, res) => {
        if (err) {
          return void callback(Error(`delete queue ${queue.name} error: ${err}`));
        } else if (res.statusCode !== 204 && res.statusCode !== 404) {
          return void callback(Error(`delete queues with status ${res.statusCode}`));
        }
        removeQueues(queues, callback);
      });
  }

  superagent
    .agent(keepAliveAgent)
    .auth('guest', 'guest')
    .get('http://localhost:15672/api/queues/%2f', (err, res) => {
      if (err) {
        return void done(Error(`get queue error: ${err}`));
      } else if (res.statusCode !== 200) {
        return void done(Error(`get queues with status ${res.statusCode}`));
      }
      removeQueues(res.body, done);
    });
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
