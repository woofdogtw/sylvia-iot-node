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
  ApplicationMgr,
  AppUlData,
  AppDlData,
  AppDlDataResp,
  AppDlDataResult,
  OnAppUlData,
  OnAppDlDataResp,
  OnAppDlDataResult,
} = require('../../mq/application');
const { MgrStatus } = require('../../mq/constants');

const lib = require('./lib');
const { connHostUri, SHARED_PREFIX } = require('./lib');

class TestHandler {
  constructor() {
    this.statusChanged = false;
    this.recvUlData = [];
    this.recvDlDataResp = [];
    this.recvDlDataResult = [];
    this.isUlDataRecv = false;
    this.isDlDataRespRecv = false;
    this.isDlDataResultRecv = false;
  }

  /** @type {OnAppUlData} */
  onUlData(_mgr, data, callback) {
    if (!this.isUlDataRecv) {
      this.isUlDataRecv = true;
      // test AMQP NACK.
      return void callback(Error(''));
    }

    this.recvUlData.push(data);
    callback(null);
  }

  /** @type {OnAppDlDataResp} */
  onDlDataResp(_mgr, data, callback) {
    if (!this.isDlDataRespRecv) {
      this.isDlDataRespRecv = true;
      // test AMQP NACK.
      return void callback(Error(''));
    }

    this.recvDlDataResp.push(data);
    callback(null);
  }

  /** @type {OnAppDlDataResult} */
  onDlDataResult(_mgr, data, callback) {
    if (!this.isDlDataResultRecv) {
      this.isDlDataResultRecv = true;
      // test AMQP NACK.
      return void callback(Error(''));
    }

    this.recvDlDataResult.push(data);
    callback(null);
  }

  /** @type {boolean} */
  statusChanged;
  /** @type {AppUlData[]} */
  recvUlData;
  /** @type {AppDlDataResp[]} */
  recvDlDataResp;
  /** @type {AppDlDataResult[]} */
  recvDlDataResult;
  /** @type {boolean} */
  isUlDataRecv;
  /** @type {boolean} */
  isDlDataRespRecv;
  /** @type {boolean} */
  isDlDataResultRecv;
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
      onUlData: () => {},
      onDlDataResp: () => {},
      onDlDataResult: () => {},
    };

    const opts = {
      unitId: 'unit_id',
      unitCode: 'unit_code',
      id: 'id_application',
      name: 'code_application',
      sharedPrefix: SHARED_PREFIX,
    };
    const mgr = new ApplicationMgr(connPool, hostUri, opts, handlers);
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
        } else if (qStatus.dldataResp !== Status.Connected) {
          return void done('dldataResp not connected');
        } else if (qStatus.dldataResult !== Status.Connected) {
          return void done('dldataResult not connected');
        }
        done(null);
      }
    });
    lib.appMgrs.push(mgr);

    assert.strictEqual(mgr.unitId(), 'unit_id');
    assert.strictEqual(mgr.unitCode(), 'unit_code');
    assert.strictEqual(mgr.id(), 'id_application');
    assert.strictEqual(mgr.name(), 'code_application');
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
      onUlData: () => {},
      onDlDataResp: () => {},
      onDlDataResult: () => {},
    };

    let complete = 0;

    const opts = {
      unitId: 'unit_id',
      unitCode: 'unit_code',
      id: 'id_application',
      name: 'code_application',
      prefetch: 0,
      persistent: false,
      sharedPrefix: SHARED_PREFIX,
    };
    const mgr = new ApplicationMgr(connPool, hostUri, opts, handlers);
    assert.ok(mgr);
    mgr.on(Events.Status, (status) => {
      if (status === MgrStatus.Ready) {
        complete++;
        if (complete >= 2) {
          done(null);
        }
      }
    });
    lib.appMgrs.push(mgr);

    const opts2 = {
      unitId: 'unit_id',
      unitCode: 'unit_code',
      id: 'id_application',
      name: 'code_application',
      prefetch: 1,
      persistent: true,
      sharedPrefix: SHARED_PREFIX,
    };
    const mgr2 = new ApplicationMgr(connPool, hostUri, opts2, handlers);
    assert.ok(mgr2);
    mgr2.on(Events.Status, (status) => {
      if (status === MgrStatus.Ready) {
        complete++;
        if (complete >= 2) {
          done(null);
        }
      }
    });
    lib.appMgrs.push(mgr2);
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
      onUlData: () => {},
      onDlDataResp: () => {},
      onDlDataResult: () => {},
    };

    assert.throws(() => {
      new ApplicationMgr({});
    });
    assert.throws(() => {
      new ApplicationMgr(connPool, 'url');
    });
    assert.throws(() => {
      new ApplicationMgr(connPool, hostUri, null);
    });
    assert.throws(() => {
      new ApplicationMgr(connPool, hostUri, { unitId: '' });
    });
    assert.throws(() => {
      new ApplicationMgr(connPool, hostUri, { unitId: 'unit_id' }, null);
    });
    assert.throws(() => {
      new ApplicationMgr(connPool, hostUri, { unitId: 'unit_id' }, {});
    });
    assert.throws(() => {
      new ApplicationMgr(connPool, new URL('tcp://localhost'), { unitId: 'unit_id' }, handlers);
    });
    const opts = {
      unitId: 1,
    };
    assert.throws(() => {
      new ApplicationMgr(connPool, hostUri, opts, handlers);
    });
    opts.unitId = 'unit_id';
    opts.unitCode = 1;
    assert.throws(() => {
      new ApplicationMgr(connPool, hostUri, opts, handlers);
    });
    opts.unitCode = 'unit_code';
    opts.id = '';
    assert.throws(() => {
      new ApplicationMgr(connPool, hostUri, opts, handlers);
    });
    opts.id = 'id';
    opts.name = '';
    assert.throws(() => {
      new ApplicationMgr(connPool, hostUri, opts, handlers);
    });
    opts.name = 'code';
    opts.unitCode = '';
    assert.throws(() => {
      new ApplicationMgr(connPool, hostUri, opts, handlers);
    });
    opts.unitCode = 'unit_code';
    opts.prefetch = 65536;
    assert.throws(() => {
      new ApplicationMgr(connPool, hostUri, opts, handlers);
    });
    delete opts.prefetch;
    opts.persistent = 0;
    assert.throws(() => {
      new ApplicationMgr(connPool, hostUri, opts, handlers);
    });
    delete opts.persistent;
    opts.sharedPrefix = null;
    assert.throws(() => {
      new ApplicationMgr(connPool, hostUri, opts, handlers);
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
      onUlData: () => {},
      onDlDataResp: () => {},
      onDlDataResult: () => {},
    };

    const opts = {
      unitId: 'unit_id',
      unitCode: 'unit_code',
      id: 'id_application',
      name: 'code_application',
      sharedPrefix: SHARED_PREFIX,
    };
    const mgr = new ApplicationMgr(connPool, hostUri, opts, handlers);
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
 * Test receiving uldata.
 *
 * @param {Engine} engine
 */
function uldata(engine) {
  return function (done) {
    /** @type {AmqpQueue|MqttQueue} */
    let queue;
    const now = new Date();
    const testHandler = new TestHandler();

    /** @type {AppUlData} */
    let data1;
    /** @type {AppUlData} */
    let data2;
    /** @type {AppUlData} */
    let data3;

    async.waterfall(
      [
        function (cb) {
          const connPool = lib.mgrConns;
          const hostUri = connHostUri(engine);
          const handlers = {
            onUlData: testHandler.onUlData.bind(testHandler),
            onDlDataResp: testHandler.onDlDataResp.bind(testHandler),
            onDlDataResult: testHandler.onDlDataResult.bind(testHandler),
          };

          let complete = 0;

          const opts = {
            unitId: 'unit_id',
            unitCode: 'unit_code',
            id: 'id_application',
            name: 'code_application',
            sharedPrefix: SHARED_PREFIX,
          };
          const mgr = new ApplicationMgr(connPool, hostUri, opts, handlers);
          lib.appMgrs.push(mgr);
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
              name: 'broker.application.unit_code.code_application.uldata',
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
            time: now.toISOString(),
            pub: new Date(now.getTime() + 1).toISOString(),
            deviceId: 'device_id1',
            networkId: 'network_id1',
            networkCode: 'network_code1',
            networkAddr: 'network_addr1',
            isPublic: true,
            data: '01',
          };
          queue.sendMsg(Buffer.from(JSON.stringify(data1)), (err) => {
            cb(err ? `send data1 error ${err}` : null);
          });
        },
        function (cb) {
          data2 = {
            dataId: '2',
            time: new Date(now.getTime() + 1).toISOString(),
            pub: new Date(now.getTime() + 2).toISOString(),
            deviceId: 'device_id2',
            networkId: 'network_id2',
            networkCode: 'network_code2',
            networkAddr: 'network_addr2',
            isPublic: false,
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
            time: new Date(now.getTime() + 2).toISOString(),
            pub: new Date(now.getTime() + 3).toISOString(),
            deviceId: 'device_id3',
            networkId: 'network_id3',
            networkCode: 'network_code3',
            networkAddr: 'network_addr3',
            isPublic: false,
            data: '',
          };
          queue.sendMsg(Buffer.from(JSON.stringify(data3)), (err) => {
            cb(err ? `send data3 error ${err}` : null);
          });
        },
        function (cb) {
          const expectCount = engine === gmq.amqp ? 3 : 2;
          (function waitFn(retry) {
            if (retry < 0) {
              return void cb(Error(`receive ${testHandler.recvUlData.length}/${expectCount} data`));
            } else if (testHandler.recvUlData.length < expectCount) {
              return void setTimeout(() => {
                waitFn(retry - 1);
              }, 10);
            }

            for (let i = 0; i < expectCount; i++) {
              const data = testHandler.recvUlData.pop();
              if (!data) {
                return void cb(Error(`only receive ${i}/${expectCount} data`));
              }
              const dataId = data.dataId;
              if (dataId === '1') {
                if (engine !== gmq.amqp) {
                  return void cb(Error('data1 wrong engine'));
                } else if (data.time.getTime() !== now.getTime()) {
                  return void cb(
                    Error(`data1.time ${data.time.toISOString()} eq ${now}.toISOString()`)
                  );
                } else if (data.pub.getTime() !== now.getTime() + 1) {
                  return void cb(
                    Error(
                      `data1.pub ${data.pub.toISOString()} eq ${new Date(
                        now.getTime() + 1
                      ).toISOString()}`
                    )
                  );
                } else if (data.deviceId !== data1.deviceId) {
                  return void cb(Error(`data1.deviceId ${data.deviceId} eq ${data1.deviceId}`));
                } else if (data.networkId !== data1.networkId) {
                  return void cb(Error(`data1.networkId ${data.networkId} eq ${data1.networkId}`));
                } else if (data.networkCode !== data1.networkCode) {
                  return void cb(
                    Error(`data1.networkCode ${data.networkCode} eq ${data1.networkCode}`)
                  );
                } else if (data.networkAddr !== data1.networkAddr) {
                  return void cb(
                    Error(`data1.networkAddr ${data.networkAddr} eq ${data1.networkAddr}`)
                  );
                } else if (data.isPublic !== data1.isPublic) {
                  return void cb(Error(`data1.isPublic ${data.isPublic} eq ${data1.isPublic}`));
                } else if (data.data.toString('hex') !== data1.data) {
                  return void cb(Error(`data1.data ${data.data.toString('hex')} eq ${data1.data}`));
                } else if (!deepEqual(data.extension, data1.extension)) {
                  return void cb(Error(`data1.extension ${data.extension} eq ${data1.extension}`));
                }
              } else if (dataId === '2') {
                if (data.time.getTime() !== now.getTime() + 1) {
                  return void cb(
                    Error(
                      `data2.time ${data.time.toISOString()} eq ${new Date(
                        now.getTime() + 1
                      ).toISOString()}`
                    )
                  );
                } else if (data.pub.getTime() !== now.getTime() + 2) {
                  return void cb(
                    Error(
                      `data2.pub ${data.pub.toISOString()} eq ${new Date(
                        now.getTime() + 2
                      ).toISOString()}`
                    )
                  );
                } else if (data.deviceId !== data2.deviceId) {
                  return void cb(Error(`data2.deviceId ${data.deviceId} eq ${data2.deviceId}`));
                } else if (data.networkId !== data2.networkId) {
                  return void cb(Error(`data2.networkId ${data.networkId} eq ${data2.networkId}`));
                } else if (data.networkCode !== data2.networkCode) {
                  return void cb(
                    Error(`data2.networkCode ${data.networkCode} eq ${data2.networkCode}`)
                  );
                } else if (data.networkAddr !== data2.networkAddr) {
                  return void cb(
                    Error(`data2.networkAddr ${data.networkAddr} eq ${data2.networkAddr}`)
                  );
                } else if (data.isPublic !== data2.isPublic) {
                  return void cb(Error(`data2.isPublic ${data.isPublic} eq ${data2.isPublic}`));
                } else if (data.data.toString('hex') !== data2.data) {
                  return void cb(Error(`data2.data ${data.data.toString('hex')} eq ${data2.data}`));
                } else if (!deepEqual(data.extension, data2.extension)) {
                  return void cb(Error(`data2.extension ${data.extension} eq ${data2.extension}`));
                }
              } else if (dataId === '3') {
                if (data.time.getTime() !== now.getTime() + 2) {
                  return void cb(
                    Error(
                      `data3.time ${data.time.toISOString()} eq ${new Date(
                        now.getTime() + 2
                      ).toISOString()}`
                    )
                  );
                } else if (data.pub.getTime() !== now.getTime() + 3) {
                  return void cb(
                    Error(
                      `data3.pub ${data.pub.toISOString()} eq ${new Date(
                        now.getTime() + 3
                      ).toISOString()}`
                    )
                  );
                } else if (data.deviceId !== data3.deviceId) {
                  return void cb(Error(`data3.deviceId ${data.deviceId} eq ${data3.deviceId}`));
                } else if (data.networkId !== data3.networkId) {
                  return void cb(Error(`data3.networkId ${data.networkId} eq ${data3.networkId}`));
                } else if (data.networkCode !== data3.networkCode) {
                  return void cb(
                    Error(`data3.networkCode ${data.networkCode} eq ${data3.networkCode}`)
                  );
                } else if (data.networkAddr !== data3.networkAddr) {
                  return void cb(
                    Error(`data3.networkAddr ${data.networkAddr} eq ${data3.networkAddr}`)
                  );
                } else if (data.isPublic !== data3.isPublic) {
                  return void cb(Error(`data3.isPublic ${data.isPublic} eq ${data3.isPublic}`));
                } else if (data.data.toString('hex') !== data3.data) {
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
 * Test receiving uldata with wrong content.
 *
 * @param {Engine} engine
 */
function uldataWrong(engine) {
  return function (done) {
    /** @type {AmqpQueue|MqttQueue} */
    let queue;
    const now = new Date();
    const testHandler = new TestHandler();

    async.waterfall(
      [
        function (cb) {
          const connPool = lib.mgrConns;
          const hostUri = connHostUri(engine);
          const handlers = {
            onUlData: testHandler.onUlData.bind(testHandler),
            onDlDataResp: testHandler.onDlDataResp.bind(testHandler),
            onDlDataResult: testHandler.onDlDataResult.bind(testHandler),
          };

          let complete = 0;

          const opts = {
            unitId: 'unit_id',
            unitCode: 'unit_code',
            id: 'id_application',
            name: 'code_application',
            sharedPrefix: SHARED_PREFIX,
          };
          const mgr = new ApplicationMgr(connPool, hostUri, opts, handlers);
          lib.appMgrs.push(mgr);
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
              name: 'broker.application.unit_code.code_application.uldata',
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
              cb(testHandler.isUlDataRecv ? Error('should not receive data') : null);
            }, 1000);
          });
        },
      ],
      done
    );
  };
}

/**
 * Test generating dldata.
 *
 * @param {Engine} engine
 */
function dldata(engine) {
  return function (done) {
    /** @type {AmqpQueue|MqttQueue} */
    let queue;
    /** @type {ApplicationMgr} */
    let mgr;
    const now = new Date();
    const testHandler = new TestHandler();

    /** @type {AppDlData} */
    let data1;
    /** @type {AppDlData} */
    let data2;

    /** @type {Buffer[]} */
    const recv_dldata = [];

    async.waterfall(
      [
        function (cb) {
          const connPool = lib.mgrConns;
          const hostUri = connHostUri(engine);
          const handlers = {
            onUlData: testHandler.onUlData.bind(testHandler),
            onDlDataResp: testHandler.onDlDataResp.bind(testHandler),
            onDlDataResult: testHandler.onDlDataResult.bind(testHandler),
          };

          let complete = 0;

          const opts = {
            unitId: 'unit_id',
            unitCode: 'unit_code',
            id: 'id_application',
            name: 'code_application',
            sharedPrefix: SHARED_PREFIX,
          };
          mgr = new ApplicationMgr(connPool, hostUri, opts, handlers);
          lib.appMgrs.push(mgr);
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
              name: 'broker.application.unit_code.code_application.dldata',
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
              recv_dldata.push(msg.payload);
              queue.ack(msg, (_err) => {});
            });
            queue.connect();
          });
        },
        function (cb) {
          data1 = {
            correlationId: '1',
            deviceId: 'device1',
            data: Buffer.from('01', 'hex'),
            extension: { key: 'value' },
          };
          mgr.sendDlData(data1, (err) => {
            cb(err || null);
          });
        },
        function (cb) {
          data2 = {
            correlationId: '2',
            networkCode: 'code',
            networkAddr: 'addr2',
            data: Buffer.from('02', 'hex'),
          };
          mgr.sendDlData(data2, (err) => {
            cb(err || null);
          });
        },
        function (cb) {
          (function waitFn(retry) {
            if (retry < 0) {
              return cb(Error(`only receive ${recv_dldata.length}/2 data`));
            } else if (recv_dldata.length === 2) {
              return cb(null);
            }
            setTimeout(() => {
              waitFn(retry - 1);
            }, 10);
          })(100);
        },
        function (cb) {
          for (let i = 0; i < 2; i++) {
            const dataBuff = recv_dldata.pop();
            if (!dataBuff) {
              return cb(Error(`gmq only receive ${i}/2 data`));
            }
            const data = JSON.parse(dataBuff.toString());
            if (data.correlationId === '1') {
              if (data.deviceId !== data1.deviceId) {
                return void cb(Error(`data1.deviceId ${data.deviceId} eq ${data1.deviceId}`));
              } else if (data.networkCode !== undefined) {
                return void cb(
                  Error(`data1.networkCode ${data.networkCode} eq ${data1.networkCode}`)
                );
              } else if (data.networkAddr !== undefined) {
                return void cb(
                  Error(`data1.networkAddr ${data.networkAddr} eq ${data1.networkAddr}`)
                );
              } else if (data.data !== data1.data.toString('hex')) {
                return void cb(Error(`data1.data ${data.data} eq ${data1.data.toString('hex')}`));
              } else if (!deepEqual(data.extension, data1.extension)) {
                return void cb(Error(`data1.extension ${data.extension} eq ${data1.extension}`));
              }
            } else if (data.correlationId === '2') {
              if (data.deviceId !== undefined) {
                return void cb(Error(`data2.deviceId ${data.deviceId} eq ${data2.deviceId}`));
              } else if (data.networkCode !== data2.networkCode) {
                return void cb(
                  Error(`data2.networkCode ${data.networkCode} eq ${data2.networkCode}`)
                );
              } else if (data.networkAddr !== data2.networkAddr) {
                return void cb(
                  Error(`data2.networkAddr ${data.networkAddr} eq ${data2.networkAddr}`)
                );
              } else if (data.data !== data2.data.toString('hex')) {
                return void cb(Error(`data2.data ${data.data} eq ${data2.data.toString('hex')}`));
              } else if (!deepEqual(data.extension, data2.extension)) {
                return void cb(Error(`data2.extension ${data.extension} eq ${data2.extension}`));
              }
            } else {
              return cb(Error(`receive wrong data ${data.correlationId}`));
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
 * Test sending dldata with wrong content.
 *
 * @param {Engine} engine
 */
function dldataWrong(engine) {
  return function (done) {
    /** @type {ApplicationMgr} */
    let mgr;
    const testHandler = new TestHandler();

    async.waterfall(
      [
        function (cb) {
          const connPool = lib.mgrConns;
          const hostUri = connHostUri(engine);
          const handlers = {
            onUlData: testHandler.onUlData.bind(testHandler),
            onDlDataResp: testHandler.onDlDataResp.bind(testHandler),
            onDlDataResult: testHandler.onDlDataResult.bind(testHandler),
          };

          const opts = {
            unitId: 'unit_id',
            unitCode: 'unit_code',
            id: 'id_application',
            name: 'code_application',
            sharedPrefix: SHARED_PREFIX,
          };
          mgr = new ApplicationMgr(connPool, hostUri, opts, handlers);
          lib.appMgrs.push(mgr);
          mgr.on(Events.Status, (status) => {
            if (status === MgrStatus.Ready) {
              cb(null);
            }
          });
        },
        function (cb) {
          try {
            mgr.sendDlData(null, (_err) => {});
            return void cb(Error('data is not an object'));
          } catch (_e) {}
          /** @type {AppDlData} */
          const data = {
            correlation: 1,
          };
          try {
            mgr.sendDlData(data, (_err) => {});
            return void cb(Error('correlationId is not a string'));
          } catch (_e) {}
          data.correlationId = '1';
          data.deviceId = 1;
          try {
            mgr.sendDlData(data, (_err) => {});
            return void cb(Error('deviceId is not a string'));
          } catch (_e) {}
          delete data.deviceId;
          data.networkCode = 1;
          try {
            mgr.sendDlData(data, (_err) => {});
            return void cb(Error('networkCode is not a string'));
          } catch (_e) {}
          data.networkCode = 'code';
          data.networkAddr = 1;
          try {
            mgr.sendDlData(data, (_err) => {});
            return void cb(Error('networkAddr is not a string'));
          } catch (_e) {}
          data.networkAddr = 'addr';
          data.data = [1];
          try {
            mgr.sendDlData(data, (_err) => {});
            return void cb(Error('data is not a buffer'));
          } catch (_e) {}
          data.data = Buffer.from('01', 'hex');
          data.extension = [];
          try {
            mgr.sendDlData(data, (_err) => {});
            return void cb(Error('extension is not an object'));
          } catch (_e) {}
          delete data.networkAddr;
          delete data.extension;
          try {
            mgr.sendDlData(data, (_err) => {});
            return void cb(Error('networkCode/networkAddr is not a string pair'));
          } catch (_e) {}
          delete data.networkCode;
          data.networkAddr = 'addr';
          try {
            mgr.sendDlData(data, (_err) => {});
            return void cb(Error('networkCode/networkAddr is not a string pair - 2'));
          } catch (_e) {}
          cb(null);
        },
      ],
      done
    );
  };
}

/**
 * Test receiving dldata-resp.
 *
 * @param {Engine} engine
 */
function dldataResp(engine) {
  return function (done) {
    /** @type {AmqpQueue|MqttQueue} */
    let queue;
    const testHandler = new TestHandler();

    /** @type {AppDlDataResp} */
    let data1;
    /** @type {AppDlDataResp} */
    let data2;
    /** @type {AppDlDataResp} */
    let data3;

    async.waterfall(
      [
        function (cb) {
          const connPool = lib.mgrConns;
          const hostUri = connHostUri(engine);
          const handlers = {
            onUlData: testHandler.onUlData.bind(testHandler),
            onDlDataResp: testHandler.onDlDataResp.bind(testHandler),
            onDlDataResult: testHandler.onDlDataResult.bind(testHandler),
          };

          let complete = 0;

          const opts = {
            unitId: 'unit_id',
            unitCode: 'unit_code',
            id: 'id_application',
            name: 'code_application',
            sharedPrefix: SHARED_PREFIX,
          };
          const mgr = new ApplicationMgr(connPool, hostUri, opts, handlers);
          lib.appMgrs.push(mgr);
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
              name: 'broker.application.unit_code.code_application.dldata-resp',
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
            correlationId: '1',
            dataId: 'data_id1',
          };
          queue.sendMsg(Buffer.from(JSON.stringify(data1)), (err) => {
            cb(err ? `send data1 error ${err}` : null);
          });
        },
        function (cb) {
          data2 = {
            correlationId: '2',
            dataId: 'data_id2',
          };
          queue.sendMsg(Buffer.from(JSON.stringify(data2)), (err) => {
            cb(err ? `send data2 error ${err}` : null);
          });
        },
        function (cb) {
          data3 = {
            correlationId: '3',
            error: 'error3',
            message: 'message3',
          };
          queue.sendMsg(Buffer.from(JSON.stringify(data3)), (err) => {
            cb(err ? `send data3 error ${err}` : null);
          });
        },
        function (cb) {
          const expectCount = engine === gmq.amqp ? 3 : 2;
          (function waitFn(retry) {
            if (retry < 0) {
              return void cb(
                Error(`receive ${testHandler.recvDlDataResp.length}/${expectCount} data`)
              );
            } else if (testHandler.recvDlDataResp.length < expectCount) {
              return void setTimeout(() => {
                waitFn(retry - 1);
              }, 10);
            }

            for (let i = 0; i < expectCount; i++) {
              const data = testHandler.recvDlDataResp.pop();
              if (!data) {
                return void cb(Error(`only receive ${i}/${expectCount} data`));
              }
              const correlationId = data.correlationId;
              if (correlationId === '1') {
                if (engine !== gmq.amqp) {
                  return void cb(Error('data1 wrong engine'));
                } else if (data.dataId !== data1.dataId) {
                  return void cb(Error(`data1.dataId ${data.dataId} eq ${data1.dataId}`));
                } else if (data.error !== data1.error) {
                  return void cb(Error(`data1.error ${data.error} eq ${data1.error}`));
                } else if (data.message !== data1.message) {
                  return void cb(Error(`data1.message ${data.message} eq ${data1.message}`));
                }
              } else if (correlationId === '2') {
                if (data.dataId !== data2.dataId) {
                  return void cb(Error(`data2.dataId ${data.dataId} eq ${data2.dataId}`));
                } else if (data.error !== data2.error) {
                  return void cb(Error(`data2.error ${data.error} eq ${data2.error}`));
                } else if (data.message !== data2.message) {
                  return void cb(Error(`data2.message ${data.message} eq ${data2.message}`));
                }
              } else if (correlationId === '3') {
                if (data.dataId !== data3.dataId) {
                  return void cb(Error(`data3.dataId ${data.dataId} eq ${data3.dataId}`));
                } else if (data.error !== data3.error) {
                  return void cb(Error(`data3.error ${data.error} eq ${data3.error}`));
                } else if (data.message !== data3.message) {
                  return void cb(Error(`data3.message ${data.message} eq ${data3.message}`));
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
 * Test receiving dldata-result.
 *
 * @param {Engine} engine
 */
function dldataResult(engine) {
  return function (done) {
    /** @type {AmqpQueue|MqttQueue} */
    let queue;
    const testHandler = new TestHandler();

    /** @type {AppDlDataResult} */
    let data1;
    /** @type {AppDlDataResult} */
    let data2;
    /** @type {AppDlDataResult} */
    let data3;

    async.waterfall(
      [
        function (cb) {
          const connPool = lib.mgrConns;
          const hostUri = connHostUri(engine);
          const handlers = {
            onUlData: testHandler.onUlData.bind(testHandler),
            onDlDataResp: testHandler.onDlDataResp.bind(testHandler),
            onDlDataResult: testHandler.onDlDataResult.bind(testHandler),
          };

          let complete = 0;

          const opts = {
            unitId: 'unit_id',
            unitCode: 'unit_code',
            id: 'id_application',
            name: 'code_application',
            sharedPrefix: SHARED_PREFIX,
          };
          const mgr = new ApplicationMgr(connPool, hostUri, opts, handlers);
          lib.appMgrs.push(mgr);
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
              name: 'broker.application.unit_code.code_application.dldata-result',
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
            status: -1,
          };
          queue.sendMsg(Buffer.from(JSON.stringify(data1)), (err) => {
            cb(err ? `send data1 error ${err}` : null);
          });
        },
        function (cb) {
          data2 = {
            dataId: '2',
            status: 0,
          };
          queue.sendMsg(Buffer.from(JSON.stringify(data2)), (err) => {
            cb(err ? `send data2 error ${err}` : null);
          });
        },
        function (cb) {
          data3 = {
            dataId: '3',
            status: 1,
            message: 'error',
          };
          queue.sendMsg(Buffer.from(JSON.stringify(data3)), (err) => {
            cb(err ? `send data3 error ${err}` : null);
          });
        },
        function (cb) {
          const expectCount = engine === gmq.amqp ? 3 : 2;
          (function waitFn(retry) {
            if (retry < 0) {
              return void cb(
                Error(`receive ${testHandler.recvDlDataResult.length}/${expectCount} data`)
              );
            } else if (testHandler.recvDlDataResult.length < expectCount) {
              return void setTimeout(() => {
                waitFn(retry - 1);
              }, 10);
            }

            for (let i = 0; i < expectCount; i++) {
              const data = testHandler.recvDlDataResult.pop();
              if (!data) {
                return void cb(Error(`only receive ${i}/${expectCount} data`));
              }
              const dataId = data.dataId;
              if (dataId === '1') {
                if (engine !== gmq.amqp) {
                  return void cb(Error('data1 wrong engine'));
                } else if (data.status !== data1.status) {
                  return void cb(Error(`data1.status ${data.status} eq ${data1.status}`));
                } else if (data.message !== data1.message) {
                  return void cb(Error(`data1.message ${data.message} eq ${data1.message}`));
                }
              } else if (dataId === '2') {
                if (data.status !== data2.status) {
                  return void cb(Error(`data2.status ${data.status} eq ${data2.status}`));
                } else if (data.message !== data2.message) {
                  return void cb(Error(`data2.message ${data.message} eq ${data2.message}`));
                }
              } else if (dataId === '3') {
                if (data.status !== data3.status) {
                  return void cb(Error(`data3.status ${data.status} eq ${data3.status}`));
                } else if (data.message !== data3.message) {
                  return void cb(Error(`data3.message ${data.message} eq ${data3.message}`));
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

module.exports = {
  newDefault,
  newManual,
  newWrongOpts,
  close,
  uldata,
  uldataWrong,
  dldata,
  dldataWrong,
  dldataResp,
  dldataResult,
};
