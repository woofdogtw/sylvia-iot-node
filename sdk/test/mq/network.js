'use strict';

const assert = require('assert');

const async = require('async');
const deepEqual = require('deep-equal');

const gmq = require('general-mq');
const { AmqpConnection } = require('general-mq/lib/amqp-connection');
const { AmqpQueue } = require('general-mq/lib/amqp-queue');
const { Events, Status } = require('general-mq/lib/constants');
const { MqttConnection } = require('general-mq/lib/mqtt-connection');
const { MqttQueue } = require('general-mq/lib/mqtt-queue');

const {
  NetworkMgr,
  NetUlData,
  NetDlData,
  NetDlDataResult,
  NetCtrlMsg,
  OnNetDlData,
} = require('../../mq/network');
const { MgrStatus } = require('../../mq/constants');

const lib = require('./lib');
const { connHostUri, SHARED_PREFIX } = require('./lib');

class TestHandler {
  constructor() {
    this.statusChanged = false;
    this.recvDlData = [];
    this.recvCtrl = [];
    this.isDlDataRecv = false;
    this.isCtrlRecv = false;
  }

  /** @type {OnNetDlData} */
  onDlData(_mgr, data, callback) {
    if (!this.isDlDataRecv) {
      this.isDlDataRecv = true;
      // test AMQP NACK.
      return void callback(Error(''));
    }

    this.recvDlData.push(data);
    callback(null);
  }

  /** @type {OnCtrl} */
  onCtrl(_mgr, data, callback) {
    if (!this.isCtrlRecv) {
      this.isCtrlRecv = true;
      // test AMQP NACK.
      return void callback(Error(''));
    }

    this.recvCtrl.push(data);
    callback(null);
  }

  /** @type {boolean} */
  statusChanged;
  /** @type {NetDlData[]} */
  recvDlData;
  /** @type {NetCtrlMsg[]} */
  recvCtrl;
  /** @type {boolean} */
  isDlDataRecv;
  /** @type {boolean} */
  isCtrlRecv;
}

/**
 * @typedef {Object} Engine
 * @property {AmqpConnection|MqttConnection} Connection
 * @property {AmqpQueue|MqttQueue} Queue
 */

/**
 * Test new managers with default options.
 *
 * @param {Engine} engine
 */
function newDefault(engine) {
  return function (done) {
    const connPool = lib.mgrConns;
    const hostUri = connHostUri(engine);
    const handlers = {
      onDlData: () => {},
      onCtrl: () => {},
    };

    const opts = {
      unitId: 'unit_id',
      unitCode: 'unit_code',
      id: 'id_network',
      name: 'code_network',
      sharedPrefix: SHARED_PREFIX,
    };
    const mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
    assert.ok(mgr);
    mgr.on(Events.Status, (status) => {
      if (status === MgrStatus.Ready) {
        const mgrStatus = mgr.status();
        if (mgrStatus !== MgrStatus.Ready) {
          return void done('manager not ready');
        }
        const qStatus = mgr.mqStatus();
        if (qStatus.uldata !== Status.Connected) {
          return void done('uldata not connected');
        } else if (qStatus.dldata !== Status.Connected) {
          return void done('dldata not connected');
        } else if (qStatus.dldataResult !== Status.Connected) {
          return void done('dldataResult not connected');
        } else if (qStatus.ctrl !== Status.Connected) {
          return void done('ctrl not connected');
        }
        done(null);
      }
    });
    lib.netMgrs.push(mgr);

    assert.strictEqual(mgr.unitId(), 'unit_id');
    assert.strictEqual(mgr.unitCode(), 'unit_code');
    assert.strictEqual(mgr.id(), 'id_network');
    assert.strictEqual(mgr.name(), 'code_network');
    assert.strictEqual(mgr.status(), MgrStatus.NotReady);
  };
}

/**
 * Test new managers with manual options.
 *
 * @param {Engine} engine
 */
function newManual(engine) {
  return function (done) {
    const connPool = lib.mgrConns;
    const hostUri = connHostUri(engine);
    const handlers = {
      onDlData: () => {},
      onCtrl: () => {},
    };

    let complete = 0;

    const opts = {
      unitId: 'unit_id',
      unitCode: 'unit_code',
      id: 'id_network',
      name: 'code_network',
      prefetch: 0,
      persistent: false,
      sharedPrefix: SHARED_PREFIX,
    };
    const mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
    assert.ok(mgr);
    mgr.on(Events.Status, (status) => {
      if (status === MgrStatus.Ready) {
        complete++;
        if (complete >= 2) {
          done(null);
        }
      }
    });
    lib.netMgrs.push(mgr);

    const opts2 = {
      unitId: 'unit_id',
      unitCode: 'unit_code',
      id: 'id_network',
      name: 'code_network',
      prefetch: 1,
      persistent: true,
      sharedPrefix: SHARED_PREFIX,
    };
    const mgr2 = new NetworkMgr(connPool, hostUri, opts2, handlers);
    assert.ok(mgr2);
    mgr2.on(Events.Status, (status) => {
      if (status === MgrStatus.Ready) {
        complete++;
        if (complete >= 2) {
          done(null);
        }
      }
    });
    lib.netMgrs.push(mgr2);
  };
}

/**
 * Test new managers with wrong options.
 *
 * @param {Engine} engine
 */
function newWrongOpts(engine) {
  return function (done) {
    const connPool = lib.mgrConns;
    const hostUri = connHostUri(engine);
    const handlers = {
      onDlData: () => {},
      onCtrl: () => {},
    };

    assert.throws(() => {
      new NetworkMgr({});
    });
    assert.throws(() => {
      new NetworkMgr(connPool, 'url');
    });
    assert.throws(() => {
      new NetworkMgr(connPool, hostUri, null);
    });
    assert.throws(() => {
      new NetworkMgr(connPool, hostUri, {}, null);
    });
    assert.throws(() => {
      new NetworkMgr(connPool, hostUri, {}, {});
    });
    assert.throws(() => {
      new NetworkMgr(connPool, new URL('tcp://localhost'), {}, handlers);
    });
    const opts = {
      unitId: 1,
    };
    assert.throws(() => {
      new NetworkMgr(connPool, hostUri, opts, handlers);
    });
    opts.unitId = 'unit_id';
    opts.unitCode = 1;
    assert.throws(() => {
      new NetworkMgr(connPool, hostUri, opts, handlers);
    });
    opts.unitCode = 'unit_code';
    opts.id = '';
    assert.throws(() => {
      new NetworkMgr(connPool, hostUri, opts, handlers);
    });
    opts.id = 'id';
    opts.name = '';
    assert.throws(() => {
      new NetworkMgr(connPool, hostUri, opts, handlers);
    });
    opts.name = 'code';
    opts.unitCode = '';
    assert.throws(() => {
      new NetworkMgr(connPool, hostUri, opts, handlers);
    });
    opts.unitCode = 'unit_code';
    opts.prefetch = 65536;
    assert.throws(() => {
      new NetworkMgr(connPool, hostUri, opts, handlers);
    });
    delete opts.prefetch;
    opts.persistent = 0;
    assert.throws(() => {
      new NetworkMgr(connPool, hostUri, opts, handlers);
    });
    delete opts.persistent;
    opts.sharedPrefix = null;
    assert.throws(() => {
      new NetworkMgr(connPool, hostUri, opts, handlers);
    });
    delete opts.sharedPrefix;

    // The following cases are only used for more coverage. The real world usage will never happen.
    const mqSdkLib = require('../../mq/lib');
    const conn = mqSdkLib.getConnection(connPool, hostUri);
    assert.throws(() => {
      mqSdkLib.newDataQueues({});
    });
    assert.throws(() => {
      mqSdkLib.newDataQueues(conn, null);
    });
    assert.throws(() => {
      mqSdkLib.newDataQueues(conn, {}, '');
    });
    assert.throws(() => {
      mqSdkLib.newDataQueues(conn, {}, 'prefix', 0);
    });
    assert.throws(() => {
      new mqSdkLib.Connection({});
    });
    mqSdkLib.removeConnection(connPool, new URL('tcp://localhost'), 0, (err) => {
      done(err || null);
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
    const connPool = lib.mgrConns;
    const hostUri = connHostUri(engine);
    const handlers = {
      onDlData: () => {},
      onCtrl: () => {},
    };

    const opts = {
      unitId: 'unit_id',
      unitCode: 'unit_code',
      id: 'id_network',
      name: 'code_network',
      sharedPrefix: SHARED_PREFIX,
    };
    const mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
    assert.ok(mgr);
    mgr.on(Events.Status, (status) => {
      if (status === MgrStatus.Ready) {
        mgr.close((err) => {
          done(err || null);
        });
      }
    });
    lib.netMgrs.push(mgr);
  };
}

/**
 * Test generating uldata.
 *
 * @param {Engine} engine
 */
function uldata(engine) {
  return function (done) {
    /** @type {AmqpQueue|MqttQueue} */
    let queue;
    /** @type {NetworkMgr} */
    let mgr;
    const now = new Date();
    const testHandler = new TestHandler();

    /** @type {NetUlData} */
    let data1;
    /** @type {NetUlData} */
    let data2;

    /** @type {Buffer[]} */
    const recv_uldata = [];

    async.waterfall(
      [
        function (cb) {
          const connPool = lib.mgrConns;
          const hostUri = connHostUri(engine);
          const handlers = {
            onDlData: testHandler.onDlData.bind(testHandler),
            onCtrl: testHandler.onCtrl.bind(testHandler),
          };

          let complete = 0;

          const opts = {
            unitId: 'unit_id',
            unitCode: 'unit_code',
            id: 'id_network',
            name: 'code_network',
            sharedPrefix: SHARED_PREFIX,
          };
          mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
          lib.netMgrs.push(mgr);
          mgr.on(Events.Status, (status) => {
            if (status === MgrStatus.Ready) {
              complete++;
              if (complete >= 2) {
                return void cb(null);
              }
            }
          });

          lib.newConnection(engine, (err, poolConn) => {
            if (err) {
              return void cb(err);
            }
            lib.appNetConn = poolConn;

            const qOpts = {
              name: 'broker.network.unit_code.code_network.uldata',
              isRecv: true,
              reliable: true,
              broadcast: false,
              prefetch: 1,
              sharedPrefix: SHARED_PREFIX,
            };
            queue = new engine.Queue(qOpts, poolConn.conn);
            lib.appNetQueues.push(queue);
            queue.on(Events.Status, (status) => {
              if (status === Status.Connected) {
                complete++;
                if (complete >= 2) {
                  return void cb(null);
                }
              }
            });
            queue.setMsgHandler((queue, msg) => {
              recv_uldata.push(msg.payload);
              queue.ack(msg, (_err) => {});
            });
            queue.connect();
          });
        },
        function (cb) {
          data1 = {
            time: now,
            networkAddr: 'addr1',
            data: Buffer.from('01', 'hex'),
            extension: { key: 'value' },
          };
          mgr.sendUlData(data1, (err) => {
            cb(err || null);
          });
        },
        function (cb) {
          data2 = {
            time: new Date(now.getTime() + 1),
            networkAddr: 'addr2',
            data: Buffer.from('02', 'hex'),
          };
          mgr.sendUlData(data2, (err) => {
            cb(err || null);
          });
        },
        function (cb) {
          (function waitFn(retry) {
            if (retry < 0) {
              return cb(Error(`only receive ${recv_uldata.length}/2 data`));
            } else if (recv_uldata.length === 2) {
              return cb(null);
            }
            setTimeout(() => {
              waitFn(retry - 1);
            }, 10);
          })(100);
        },
        function (cb) {
          for (let i = 0; i < 2; i++) {
            const dataBuff = recv_uldata.pop();
            if (!dataBuff) {
              return cb(Error(`gmq only receive ${i}/2 data`));
            }
            const data = JSON.parse(dataBuff.toString());
            if (data.networkAddr === 'addr1') {
              if (data.time !== data1.time.toISOString()) {
                return void cb(Error(`data1.time ${data.time} eq ${data1.time.toISOString()}`));
              } else if (data.data !== data1.data.toString('hex')) {
                return void cb(Error(`data1.data ${data.data} eq ${data1.data.toString('hex')}`));
              } else if (!deepEqual(data.extension, data1.extension)) {
                return void cb(Error(`data1.extension ${data.extension} eq ${data1.extension}`));
              }
            } else if (data.networkAddr === 'addr2') {
              if (data.time !== data2.time.toISOString()) {
                return void cb(Error(`data2.time ${data.time} eq ${data2.time.toISOString()}`));
              } else if (data.data !== data2.data.toString('hex')) {
                return void cb(Error(`data2.data ${data.data} eq ${data2.data.toString('hex')}`));
              } else if (!deepEqual(data.extension, data2.extension)) {
                return void cb(Error(`data2.extension ${data.extension} eq ${data2.extension}`));
              }
            } else {
              return cb(Error(`receive wrong data ${data.networkAddr}`));
            }
          }
          cb(null);
        },
      ],
      done
    );
  };
}

/**
 * Test sending uldata with wrong content.
 *
 * @param {Engine} engine
 */
function uldataWrong(engine) {
  return function (done) {
    /** @type {NetworkMgr} */
    let mgr;
    const now = new Date();
    const testHandler = new TestHandler();

    async.waterfall(
      [
        function (cb) {
          const connPool = lib.mgrConns;
          const hostUri = connHostUri(engine);
          const handlers = {
            onDlData: testHandler.onDlData.bind(testHandler),
            onCtrl: testHandler.onCtrl.bind(testHandler),
          };

          const opts = {
            unitId: 'unit_id',
            unitCode: 'unit_code',
            id: 'id_network',
            name: 'code_network',
            sharedPrefix: SHARED_PREFIX,
          };
          mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
          lib.netMgrs.push(mgr);
          mgr.on(Events.Status, (status) => {
            if (status === MgrStatus.Ready) {
              cb(null);
            }
          });
        },
        function (cb) {
          try {
            mgr.sendUlData(null, (_err) => {});
            return void cb(Error('data is not an object'));
          } catch (_e) {}
          /** @type {NetUlData} */
          const data = {
            time: 1,
          };
          try {
            mgr.sendUlData(data, (_err) => {});
            return void cb(Error('time is not a Date'));
          } catch (_e) {}
          data.time = now;
          try {
            mgr.sendUlData(data, (_err) => {});
            return void cb(Error('networkAddr is not a string'));
          } catch (_e) {}
          data.networkAddr = 'addr';
          data.data = [1];
          try {
            mgr.sendUlData(data, (_err) => {});
            return void cb(Error('data is not a Buffer'));
          } catch (_e) {}
          data.data = Buffer.from('01', 'hex');
          data.extension = [];
          try {
            mgr.sendUlData(data, (_err) => {});
            return void cb(Error('extension is not an object'));
          } catch (_e) {}
          cb(null);
        },
      ],
      done
    );
  };
}

/**
 * Test receiving dldata.
 *
 * @param {Engine} engine
 */
function dldata(engine) {
  return function (done) {
    /** @type {AmqpQueue|MqttQueue} */
    let queue;
    const now = new Date();
    const testHandler = new TestHandler();

    let data1;
    let data2;
    let data3;

    async.waterfall(
      [
        function (cb) {
          const connPool = lib.mgrConns;
          const hostUri = connHostUri(engine);
          const handlers = {
            onDlData: testHandler.onDlData.bind(testHandler),
            onCtrl: testHandler.onCtrl.bind(testHandler),
          };

          let complete = 0;

          const opts = {
            unitId: 'unit_id',
            unitCode: 'unit_code',
            id: 'id_network',
            name: 'code_network',
            sharedPrefix: SHARED_PREFIX,
          };
          const mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
          lib.netMgrs.push(mgr);
          mgr.on(Events.Status, (status) => {
            if (status === MgrStatus.Ready) {
              complete++;
              if (complete >= 2) {
                return void cb(null);
              }
            }
          });

          lib.newConnection(engine, (err, poolConn) => {
            if (err) {
              return void cb(err);
            }
            lib.appNetConn = poolConn;

            const qOpts = {
              name: 'broker.network.unit_code.code_network.dldata',
              isRecv: false,
              reliable: true,
              broadcast: false,
            };
            queue = new engine.Queue(qOpts, poolConn.conn);
            lib.appNetQueues.push(queue);
            queue.on(Events.Status, (status) => {
              if (status === Status.Connected) {
                complete++;
                if (complete >= 2) {
                  return void cb(null);
                }
              }
            });
            queue.connect();
          });
        },
        function (cb) {
          data1 = {
            dataId: '1',
            pub: now.toISOString(),
            expiresIn: 1000,
            networkAddr: 'addr1',
            data: '01',
          };
          queue.sendMsg(Buffer.from(JSON.stringify(data1)), (err) => {
            cb(err ? `send data1 error ${err}` : null);
          });
        },
        function (cb) {
          data2 = {
            dataId: '2',
            pub: new Date(now.getTime() + 1).toISOString(),
            expiresIn: 2000,
            networkAddr: 'addr2',
            data: '02',
            extension: { key: 'value' },
          };
          queue.sendMsg(Buffer.from(JSON.stringify(data2)), (err) => {
            cb(err ? `send data2 error ${err}` : null);
          });
        },
        function (cb) {
          data3 = {
            dataId: '3',
            pub: new Date(now.getTime() + 2).toISOString(),
            expiresIn: 3000,
            networkAddr: 'addr3',
            data: '03',
          };
          queue.sendMsg(Buffer.from(JSON.stringify(data3)), (err) => {
            cb(err ? `send data3 error ${err}` : null);
          });
        },
        function (cb) {
          const expectCount = engine === gmq.amqp ? 3 : 2;
          (function waitFn(retry) {
            if (retry < 0) {
              return void cb(Error(`receive ${testHandler.recvDlData.length}/${expectCount} data`));
            } else if (testHandler.recvDlData.length < expectCount) {
              return void setTimeout(() => {
                waitFn(retry - 1);
              }, 10);
            }

            for (let i = 0; i < expectCount; i++) {
              const data = testHandler.recvDlData.pop();
              if (!data) {
                return void cb(Error(`only receive ${i}/${expectCount} data`));
              }
              const dataId = data.dataId;
              if (dataId === '1') {
                if (engine !== gmq.amqp) {
                  return void cb(Error('data1 wrong engine'));
                } else if (data.pub.getTime() !== now.getTime()) {
                  return void cb(
                    Error(`data1.pub ${data.pub.toISOString()} eq ${now.toISOString()}`)
                  );
                } else if (data.networkAddr !== data1.networkAddr) {
                  return void cb(
                    Error(`data1.networkAddr ${data.networkAddr} eq ${data1.networkAddr}`)
                  );
                } else if (data.data.toString('hex') !== data1.data) {
                  return void cb(Error(`data1.data ${data.data.toString('hex')} eq ${data1.data}`));
                } else if (!deepEqual(data.extension, data1.extension)) {
                  return void cb(Error(`data1.extension ${data.extension} eq ${data1.extension}`));
                }
              } else if (dataId === '2') {
                if (data.pub.getTime() !== now.getTime() + 1) {
                  return void cb(
                    Error(
                      `data2.pub ${data.pub.toISOString()} eq ${new Date(
                        now.getTime() + 1
                      ).toISOString()}`
                    )
                  );
                } else if (data.networkAddr !== data2.networkAddr) {
                  return void cb(
                    Error(`data2.networkAddr ${data.networkAddr} eq ${data2.networkAddr}`)
                  );
                } else if (data.data.toString('hex') !== data2.data) {
                  return void cb(Error(`data2.data ${data.data.toString('hex')} eq ${data2.data}`));
                } else if (!deepEqual(data.extension, data2.extension)) {
                  return void cb(Error(`data2.extension ${data.extension} eq ${data2.extension}`));
                }
              } else if (dataId === '3') {
                if (data.pub.getTime() !== now.getTime() + 2) {
                  return void cb(
                    Error(
                      `data3.pub ${data.pub.toISOString()} eq ${new Date(
                        now.getTime() + 2
                      ).toISOString()}`
                    )
                  );
                } else if (data.networkAddr !== data3.networkAddr) {
                  return void cb(
                    Error(`data3.networkAddr ${data.networkAddr} eq ${data3.networkAddr}`)
                  );
                } else if (data.isPublic !== data3.isPublic) {
                  return void cb(Error(`data3.data ${data.data.toString('hex')} eq ${data3.data}`));
                } else if (!deepEqual(data.extension, data3.extension)) {
                  return void cb(Error(`data3.extension ${data.extension} eq ${data3.extension}`));
                }
              }
            }
            cb(null);
          })(100);
        },
      ],
      done
    );
  };
}

/**
 * Test receiving dldata with wrong content.
 *
 * @param {Engine} engine
 */
function dldataWrong(engine) {
  return function (done) {
    /** @type {AmqpQueue|MqttQueue} */
    let queue;
    const testHandler = new TestHandler();

    async.waterfall(
      [
        function (cb) {
          const connPool = lib.mgrConns;
          const hostUri = connHostUri(engine);
          const handlers = {
            onDlData: testHandler.onDlData.bind(testHandler),
            onCtrl: testHandler.onCtrl.bind(testHandler),
          };

          let complete = 0;

          const opts = {
            unitId: 'unit_id',
            unitCode: 'unit_code',
            id: 'id_network',
            name: 'code_network',
            sharedPrefix: SHARED_PREFIX,
          };
          const mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
          lib.netMgrs.push(mgr);
          mgr.on(Events.Status, (status) => {
            if (status === MgrStatus.Ready) {
              complete++;
              if (complete >= 2) {
                return void cb(null);
              }
            }
          });

          lib.newConnection(engine, (err, poolConn) => {
            if (err) {
              return void cb(err);
            }
            lib.appNetConn = poolConn;

            const qOpts = {
              name: 'broker.network.unit_code.code_network.dldata',
              isRecv: false,
              reliable: true,
              broadcast: false,
            };
            queue = new engine.Queue(qOpts, poolConn.conn);
            lib.appNetQueues.push(queue);
            queue.on(Events.Status, (status) => {
              if (status === Status.Connected) {
                complete++;
                if (complete >= 2) {
                  return void cb(null);
                }
              }
            });
            queue.connect();
          });
        },
        function (cb) {
          queue.sendMsg(Buffer.from('{'), (err) => {
            if (err) {
              return void cb(Error(`send data error ${err}`));
            }
            setTimeout(() => {
              cb(testHandler.isDlDataRecv ? Error('should not receive data') : null);
            }, 1000);
          });
        },
      ],
      done
    );
  };
}

/**
 * Test generating dldata-result.
 *
 * @param {Engine} engine
 */
function dldataResult(engine) {
  return function (done) {
    /** @type {AmqpQueue|MqttQueue} */
    let queue;
    /** @type {NetworkMgr} */
    let mgr;
    const testHandler = new TestHandler();

    /** @type {NetDlDataResult} */
    let data1;
    /** @type {NetDlDataResult} */
    let data2;

    /** @type {Buffer[]} */
    const recv_dldata_result = [];

    async.waterfall(
      [
        function (cb) {
          const connPool = lib.mgrConns;
          const hostUri = connHostUri(engine);
          const handlers = {
            onDlData: testHandler.onDlData.bind(testHandler),
            onCtrl: testHandler.onCtrl.bind(testHandler),
          };

          let complete = 0;

          const opts = {
            unitId: 'unit_id',
            unitCode: 'unit_code',
            id: 'id_network',
            name: 'code_network',
            sharedPrefix: SHARED_PREFIX,
          };
          mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
          lib.netMgrs.push(mgr);
          mgr.on(Events.Status, (status) => {
            if (status === MgrStatus.Ready) {
              complete++;
              if (complete >= 2) {
                return void cb(null);
              }
            }
          });

          lib.newConnection(engine, (err, poolConn) => {
            if (err) {
              return void cb(err);
            }
            lib.appNetConn = poolConn;

            const qOpts = {
              name: 'broker.network.unit_code.code_network.dldata-result',
              isRecv: true,
              reliable: true,
              broadcast: false,
              prefetch: 1,
              sharedPrefix: SHARED_PREFIX,
            };
            queue = new engine.Queue(qOpts, poolConn.conn);
            lib.appNetQueues.push(queue);
            queue.on(Events.Status, (status) => {
              if (status === Status.Connected) {
                complete++;
                if (complete >= 2) {
                  return void cb(null);
                }
              }
            });
            queue.setMsgHandler((queue, msg) => {
              recv_dldata_result.push(msg.payload);
              queue.ack(msg, (_err) => {});
            });
            queue.connect();
          });
        },
        function (cb) {
          data1 = {
            dataId: '1',
            status: -1,
          };
          mgr.sendDlDataResult(data1, (err) => {
            cb(err || null);
          });
        },
        function (cb) {
          data2 = {
            dataId: '2',
            status: 1,
            message: 'error',
          };
          mgr.sendDlDataResult(data2, (err) => {
            cb(err || null);
          });
        },
        function (cb) {
          (function waitFn(retry) {
            if (retry < 0) {
              return cb(Error(`only receive ${recv_dldata_result.length}/2 data`));
            } else if (recv_dldata_result.length === 2) {
              return cb(null);
            }
            setTimeout(() => {
              waitFn(retry - 1);
            }, 10);
          })(100);
        },
        function (cb) {
          for (let i = 0; i < 2; i++) {
            const dataBuff = recv_dldata_result.pop();
            if (!dataBuff) {
              return cb(Error(`gmq only receive ${i}/2 data`));
            }
            const data = JSON.parse(dataBuff.toString());
            if (data.dataId === '1') {
              if (data.status !== data1.status) {
                return void cb(Error(`data1.status ${data.status} eq ${data1.status}`));
              } else if (data.message !== data1.message) {
                return void cb(Error(`data1.message ${data.message} eq ${data1.message}`));
              }
            } else if (data.dataId === '2') {
              if (data.status !== data2.status) {
                return void cb(Error(`data2.status ${data.status} eq ${data2.status}`));
              } else if (data.message !== data2.message) {
                return void cb(Error(`data2.message ${data.message} eq ${data2.message}`));
              }
            } else {
              return cb(Error(`receive wrong data ${data.dataId}`));
            }
          }
          cb(null);
        },
      ],
      done
    );
  };
}

/**
 * Test generating dldata-result with wrong content.
 *
 * @param {Engine} engine
 */
function dldataResultWrong(engine) {
  return function (done) {
    /** @type {NetworkMgr} */
    let mgr;
    const testHandler = new TestHandler();

    async.waterfall(
      [
        function (cb) {
          const connPool = lib.mgrConns;
          const hostUri = connHostUri(engine);
          const handlers = {
            onDlData: testHandler.onDlData.bind(testHandler),
            onCtrl: testHandler.onCtrl.bind(testHandler),
          };

          const opts = {
            unitId: 'unit_id',
            unitCode: 'unit_code',
            id: 'id_network',
            name: 'code_network',
            sharedPrefix: SHARED_PREFIX,
          };
          mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
          lib.netMgrs.push(mgr);
          mgr.on(Events.Status, (status) => {
            cb(null);
          });
        },
        function (cb) {
          try {
            mgr.sendDlDataResult(null, (_err) => {});
            return void cb(Error('data is not an object'));
          } catch (_e) {}
          /** @type {NetDlDataResult} */
          const data = {};
          try {
            mgr.sendDlDataResult(data, (_err) => {});
            return void cb(Error('dataId is not a string'));
          } catch (_e) {}
          data.dataId = '1';
          try {
            mgr.sendDlDataResult(data, (_err) => {});
            return void cb(Error('status is not an integer'));
          } catch (_e) {}
          data.status = 0;
          data.message = 1;
          try {
            mgr.sendDlDataResult(data, (_err) => {});
            return void cb(Error('message is not a string'));
          } catch (_e) {}
          cb(null);
        },
      ],
      done
    );
  };
}

/**
 * Test receiving ctrl.
 *
 * @param {Engine} engine
 */
function ctrl(engine) {
  return function (done) {
    /** @type {AmqpQueue|MqttQueue} */
    let queue;
    const now = new Date();
    const testHandler = new TestHandler();

    let data1;
    let data2;
    let data3;
    let data4;
    let data5;
    let data6;

    async.waterfall(
      [
        function (cb) {
          const connPool = lib.mgrConns;
          const hostUri = connHostUri(engine);
          const handlers = {
            onDlData: testHandler.onDlData.bind(testHandler),
            onCtrl: testHandler.onCtrl.bind(testHandler),
          };

          let complete = 0;

          const opts = {
            unitId: 'unit_id',
            unitCode: 'unit_code',
            id: 'id_network',
            name: 'code_network',
            sharedPrefix: SHARED_PREFIX,
          };
          const mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
          lib.netMgrs.push(mgr);
          mgr.on(Events.Status, (status) => {
            if (status === MgrStatus.Ready) {
              complete++;
              if (complete >= 2) {
                return void cb(null);
              }
            }
          });

          lib.newConnection(engine, (err, poolConn) => {
            if (err) {
              return void cb(err);
            }
            lib.appNetConn = poolConn;

            const qOpts = {
              name: 'broker.network.unit_code.code_network.ctrl',
              isRecv: false,
              reliable: true,
              broadcast: false,
            };
            queue = new engine.Queue(qOpts, poolConn.conn);
            lib.appNetQueues.push(queue);
            queue.on(Events.Status, (status) => {
              if (status === Status.Connected) {
                complete++;
                if (complete >= 2) {
                  return void cb(null);
                }
              }
            });
            queue.connect();
          });
        },
        function (cb) {
          data1 = {
            operation: 'add-device',
            time: now.toISOString(),
            new: { networkAddr: 'addr1' },
          };
          queue.sendMsg(Buffer.from(JSON.stringify(data1)), (err) => {
            cb(err ? `send data1 error ${err}` : null);
          });
        },
        function (cb) {
          data2 = {
            operation: 'add-device-bulk',
            time: new Date(now.getTime() + 1).toISOString(),
            new: { networkAddrs: ['addr2'] },
          };
          queue.sendMsg(Buffer.from(JSON.stringify(data2)), (err) => {
            cb(err ? `send data2 error ${err}` : null);
          });
        },
        function (cb) {
          data3 = {
            operation: 'add-device-range',
            time: new Date(now.getTime() + 2).toISOString(),
            new: {
              startAddr: '0001',
              endAddr: '0002',
            },
          };
          queue.sendMsg(Buffer.from(JSON.stringify(data3)), (err) => {
            cb(err ? `send data3 error ${err}` : null);
          });
        },
        function (cb) {
          data4 = {
            operation: 'del-device',
            time: new Date(now.getTime() + 3).toISOString(),
            new: { networkAddr: 'addr4' },
          };
          queue.sendMsg(Buffer.from(JSON.stringify(data4)), (err) => {
            cb(err ? `send data4 error ${err}` : null);
          });
        },
        function (cb) {
          data5 = {
            operation: 'del-device-bulk',
            time: new Date(now.getTime() + 4).toISOString(),
            new: { networkAddrs: ['addr5'] },
          };
          queue.sendMsg(Buffer.from(JSON.stringify(data5)), (err) => {
            cb(err ? `send data5 error ${err}` : null);
          });
        },
        function (cb) {
          data6 = {
            operation: 'del-device-range',
            time: new Date(now.getTime() + 5).toISOString(),
            new: {
              startAddr: '0003',
              endAddr: '0004',
            },
          };
          queue.sendMsg(Buffer.from(JSON.stringify(data6)), (err) => {
            cb(err ? `send data6 error ${err}` : null);
          });
        },
        function (cb) {
          const expectCount = engine === gmq.amqp ? 6 : 5;
          (function waitFn(retry) {
            if (retry < 0) {
              return void cb(Error(`receive ${testHandler.recvCtrl.length}/${expectCount} data`));
            } else if (testHandler.recvCtrl.length < expectCount) {
              return void setTimeout(() => {
                waitFn(retry - 1);
              }, 10);
            }

            let recv_dev_add = false;
            let recv_dev_add_bulk = false;
            let recv_dev_add_range = false;
            let recv_dev_del = false;
            let recv_dev_del_bulk = false;
            let recv_dev_del_range = false;
            for (let i = 0; i < expectCount; i++) {
              const data = testHandler.recvCtrl.pop();
              if (!data) {
                return void cb(Error(`only receive ${i}/${expectCount} data`));
              }
              const operation = data.operation;
              if (operation === 'add-device') {
                if (engine !== gmq.amqp) {
                  return void cb(Error('data1 wrong engine'));
                } else if (data.time.getTime() !== now.getTime()) {
                  return void cb(
                    Error(`data1.time ${data.time.toISOString()} eq ${now.toISOString()}`)
                  );
                } else if (!deepEqual(data.new, data1.new)) {
                  return void cb(Error(`data1.new ${data.new} eq ${data1.new}`));
                }
                recv_dev_add = true;
              } else if (operation === 'add-device-bulk') {
                if (data.time.getTime() !== new Date(now.getTime() + 1).getTime()) {
                  return void cb(
                    Error(
                      `data2.time ${data.time.toISOString()} eq ${new Date(
                        now.getTime() + 1
                      ).toISOString()}`
                    )
                  );
                } else if (!deepEqual(data.new, data2.new)) {
                  return void cb(Error(`data2.new ${data.new} eq ${data2.new}`));
                }
                recv_dev_add_bulk = true;
              } else if (operation === 'add-device-range') {
                if (data.time.getTime() !== new Date(now.getTime() + 2).getTime()) {
                  return void cb(
                    Error(
                      `data3.time ${data.time.toISOString()} eq ${new Date(
                        now.getTime() + 2
                      ).toISOString()}`
                    )
                  );
                } else if (!deepEqual(data.new, data3.new)) {
                  return void cb(Error(`data3.new ${data.new} eq ${data3.new}`));
                }
                recv_dev_add_range = true;
              } else if (operation === 'del-device') {
                if (data.time.getTime() !== new Date(now.getTime() + 3).getTime()) {
                  return void cb(
                    Error(
                      `data4.time ${data.time.toISOString()} eq ${new Date(
                        now.getTime() + 3
                      ).toISOString()}`
                    )
                  );
                } else if (!deepEqual(data.new, data4.new)) {
                  return void cb(Error(`data4.new ${data.new} eq ${data4.new}`));
                }
                recv_dev_del = true;
              } else if (operation === 'del-device-bulk') {
                if (data.time.getTime() !== new Date(now.getTime() + 4).getTime()) {
                  return void cb(
                    Error(
                      `data5.time ${data.time.toISOString()} eq ${new Date(
                        now.getTime() + 4
                      ).toISOString()}`
                    )
                  );
                } else if (!deepEqual(data.new, data5.new)) {
                  return void cb(Error(`data5.new ${data.new} eq ${data5.new}`));
                }
                recv_dev_del_bulk = true;
              } else if (operation === 'del-device-range') {
                if (data.time.getTime() !== new Date(now.getTime() + 5).getTime()) {
                  return void cb(
                    Error(
                      `data6.time ${data.time.toISOString()} eq ${new Date(
                        now.getTime() + 5
                      ).toISOString()}`
                    )
                  );
                } else if (!deepEqual(data.new, data6.new)) {
                  return void cb(Error(`data6.new ${data.new} eq ${data6.new}`));
                }
                recv_dev_del_range = true;
              }
            }
            const result =
              (recv_dev_add || engine !== gmq.amqp) &&
              recv_dev_add_bulk &&
              recv_dev_add_range &&
              recv_dev_del &&
              recv_dev_del_bulk &&
              recv_dev_del_range;
            cb(result ? null : Error('not recv all'));
          })(100);
        },
      ],
      done
    );
  };
}

module.exports = {
  newDefault,
  newManual,
  newWrongOpts,
  close,
  uldata,
  uldataWrong,
  dldata,
  dldataWrong,
  dldataResult,
  dldataResultWrong,
  ctrl,
};
