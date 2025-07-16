'use strict';

const assert = require('assert');

const gmq = require('general-mq');
const { AmqpConnection, AmqpQueue, MqttConnection, MqttQueue } = gmq;
const { Events, Status } = gmq.constants;

const {
  NetworkMgr,
  NetUlData,
  NetDlData,
  NetDlDataResult,
  NetCtrlMsg,
  OnNetDlData,
} = require('../../mq/network');
const mqSdkLib = require('../../mq/lib');
const { MgrStatus } = require('../../mq/constants');

const lib = require('./lib');
const { connHostUri, SHARED_PREFIX } = lib;

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
  return async function () {
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
    lib.netMgrs.push(mgr);

    assert.strictEqual(mgr.unitId(), 'unit_id');
    assert.strictEqual(mgr.unitCode(), 'unit_code');
    assert.strictEqual(mgr.id(), 'id_network');
    assert.strictEqual(mgr.name(), 'code_network');
    assert.strictEqual(mgr.status(), MgrStatus.NotReady);

    await new Promise((resolve) => {
      mgr.on(Events.Status, (status) => {
        if (status === MgrStatus.Ready) {
          const mgrStatus = mgr.status();
          assert.strictEqual(mgrStatus, MgrStatus.Ready, 'manager not ready');
          const qStatus = mgr.mqStatus();
          assert.strictEqual(qStatus.uldata, Status.Connected, 'uldata not connected');
          assert.strictEqual(qStatus.dldata, Status.Connected, 'dldata not connected');
          assert.strictEqual(qStatus.dldataResult, Status.Connected, 'dldataResult not connected');
          assert.strictEqual(qStatus.ctrl, Status.Connected, 'ctrl not connected');
          resolve();
        }
      });
    });
  };
}

/**
 * Test new managers with manual options.
 *
 * @param {Engine} engine
 */
function newManual(engine) {
  return async function () {
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
      prefetch: 0,
      persistent: false,
      sharedPrefix: SHARED_PREFIX,
    };
    const mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
    assert.ok(mgr);
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
    lib.netMgrs.push(mgr2);

    await new Promise((resolve) => {
      let complete = 0;
      mgr.on(Events.Status, (status) => {
        if (status === MgrStatus.Ready) {
          complete++;
          if (complete >= 2) {
            resolve();
          }
        }
      });
      mgr2.on(Events.Status, (status) => {
        if (status === MgrStatus.Ready) {
          complete++;
          if (complete >= 2) {
            resolve();
          }
        }
      });
    });
  };
}

/**
 * Test new managers with wrong options.
 *
 * @param {Engine} engine
 */
function newWrongOpts(engine) {
  return async function () {
    const connPool = lib.mgrConns;
    const hostUri = connHostUri(engine);
    const handlers = {
      onDlData: () => {},
      onCtrl: () => {},
    };

    assert.throws(() => new NetworkMgr({}));
    assert.throws(() => new NetworkMgr(connPool, 'url'));
    assert.throws(() => new NetworkMgr(connPool, hostUri, null));
    assert.throws(() => new NetworkMgr(connPool, hostUri, {}, null));
    assert.throws(() => new NetworkMgr(connPool, hostUri, {}, {}));
    assert.throws(() => new NetworkMgr(connPool, new URL('tcp://localhost'), {}, handlers));
    const opts = {
      unitId: 1,
    };
    assert.throws(() => new NetworkMgr(connPool, hostUri, opts, handlers));
    opts.unitId = 'unit_id';
    opts.unitCode = 1;
    assert.throws(() => new NetworkMgr(connPool, hostUri, opts, handlers));
    opts.unitCode = 'unit_code';
    opts.id = '';
    assert.throws(() => new NetworkMgr(connPool, hostUri, opts, handlers));
    opts.id = 'id';
    opts.name = '';
    assert.throws(() => new NetworkMgr(connPool, hostUri, opts, handlers));
    opts.name = 'code';
    opts.unitCode = '';
    assert.throws(() => new NetworkMgr(connPool, hostUri, opts, handlers));
    opts.unitCode = 'unit_code';
    opts.prefetch = 65536;
    assert.throws(() => new NetworkMgr(connPool, hostUri, opts, handlers));
    delete opts.prefetch;
    opts.persistent = 0;
    assert.throws(() => new NetworkMgr(connPool, hostUri, opts, handlers));
    delete opts.persistent;
    opts.sharedPrefix = null;
    assert.throws(() => new NetworkMgr(connPool, hostUri, opts, handlers));
    delete opts.sharedPrefix;

    // The following cases are only used for more coverage. The real world usage will never happen.
    const conn = mqSdkLib.getConnection(connPool, hostUri);
    assert.throws(() => mqSdkLib.newDataQueues({}));
    assert.throws(() => mqSdkLib.newDataQueues(conn, null));
    assert.throws(() => mqSdkLib.newDataQueues(conn, {}, ''));
    assert.throws(() => mqSdkLib.newDataQueues(conn, {}, 'prefix', 0));
    assert.throws(() => new mqSdkLib.Connection({}));
    await mqSdkLib.removeConnection(connPool, new URL('tcp://localhost'), 0);
  };
}

/**
 * Test `close()`.
 *
 * @param {Engine} engine
 */
function close(engine) {
  return async function () {
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
    lib.netMgrs.push(mgr);
    await new Promise((resolve, reject) => {
      mgr.on(Events.Status, (status) => {
        if (status === MgrStatus.Ready) {
          mgr.close().then(resolve).catch(reject);
        }
      });
    });
  };
}

/**
 * Test generating uldata.
 *
 * @param {Engine} engine
 */
function uldata(engine) {
  return async function () {
    const now = new Date();
    const testHandler = new TestHandler();

    /** @type {NetUlData} */
    let data1;
    /** @type {NetUlData} */
    let data2;

    /** @type {Buffer[]} */
    const recvUldata = [];

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
    const mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
    lib.netMgrs.push(mgr);

    lib.appNetConn = await lib.newConnection(engine);
    const qOpts = {
      name: 'broker.network.unit_code.code_network.uldata',
      isRecv: true,
      reliable: true,
      broadcast: false,
      prefetch: 1,
      sharedPrefix: SHARED_PREFIX,
    };
    const queue = new engine.Queue(qOpts, lib.appNetConn.conn);
    lib.appNetQueues.push(queue);
    queue.setMsgHandler((queue, msg) => {
      recvUldata.push(msg.payload);
      queue.ack(msg, (_err) => {});
    });

    await new Promise((resolve) => {
      let complete = 0;
      mgr.on(Events.Status, (status) => {
        if (status === MgrStatus.Ready) {
          complete++;
          if (complete >= 2) {
            return void resolve();
          }
        }
      });
      queue.on(Events.Status, (status) => {
        if (status === Status.Connected) {
          complete++;
          if (complete >= 2) {
            return void resolve();
          }
        }
      });
      queue.connect();
    });

    data1 = {
      time: now,
      networkAddr: 'addr1',
      data: Buffer.from('01', 'hex'),
      extension: { key: 'value' },
    };
    await mgr.sendUlData(data1);
    data2 = {
      time: new Date(now.getTime() + 1),
      networkAddr: 'addr2',
      data: Buffer.from('02', 'hex'),
    };
    await mgr.sendUlData(data2);

    for (let i = 0; i < 100; i++) {
      if (recvUldata.length === 2) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(recvUldata.length, 2, `receive ${recvUldata.length}/2 data`);

    for (let i = 0; i < 2; i++) {
      const dataBuff = recvUldata.pop();
      assert.ok(dataBuff, `gmq only receive ${i}/2 data`);
      const data = JSON.parse(dataBuff.toString());
      assert.ok(
        ['addr1', 'addr2'].includes(data.networkAddr),
        `receive wrong data ${data.networkAddr}`
      );
      if (data.networkAddr === 'addr1') {
        assert.strictEqual(
          data.time,
          data1.time.toISOString(),
          `data1.time ${data.time} eq ${data1.time.toISOString()}`
        );
        assert.strictEqual(
          data.data,
          data1.data.toString('hex'),
          `data1.data ${data.data} eq ${data1.data.toString('hex')}`
        );
        assert.deepStrictEqual(
          data.extension,
          data1.extension,
          `data1.extension ${data.extension} eq ${data1.extension}`
        );
      } else if (data.networkAddr === 'addr2') {
        assert.strictEqual(
          data.time,
          data2.time.toISOString(),
          `data2.time ${data.time} eq ${data2.time.toISOString()}`
        );
        assert.strictEqual(
          data.data,
          data2.data.toString('hex'),
          `data2.data ${data.data} eq ${data2.data.toString('hex')}`
        );
        assert.deepStrictEqual(
          data.extension,
          data2.extension,
          `data2.extension ${data.extension} eq ${data2.extension}`
        );
      }
    }
  };
}

/**
 * Test sending uldata with wrong content.
 *
 * @param {Engine} engine
 */
function uldataWrong(engine) {
  return async function () {
    const now = new Date();
    const testHandler = new TestHandler();

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
    const mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
    lib.netMgrs.push(mgr);
    await new Promise((resolve) => {
      mgr.on(Events.Status, (status) => {
        if (status === MgrStatus.Ready) {
          return void resolve();
        }
      });
    });

    const shouldErrFn = () => {
      throw Error('should error');
    };
    const catchFn = (err) => assert.ok(!(err instanceof SdkError));

    mgr.sendUlData(null).then(shouldErrFn).catch(catchFn);
    /** @type {NetUlData} */
    const data = {
      time: 1,
    };
    mgr.sendUlData(data).then(shouldErrFn).catch(catchFn);
    data.time = now;
    mgr.sendUlData(data).then(shouldErrFn).catch(catchFn);
    data.networkAddr = 'addr';
    data.data = [1];
    mgr.sendUlData(data).then(shouldErrFn).catch(catchFn);
    data.data = Buffer.from('01', 'hex');
    data.extension = [];
    mgr.sendUlData(data).then(shouldErrFn).catch(catchFn);
  };
}

/**
 * Test receiving dldata.
 *
 * @param {Engine} engine
 */
function dldata(engine) {
  return async function () {
    const now = new Date();
    const testHandler = new TestHandler();

    let data1;
    let data2;
    let data3;

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
    const mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
    lib.netMgrs.push(mgr);

    lib.appNetConn = await lib.newConnection(engine);
    const qOpts = {
      name: 'broker.network.unit_code.code_network.dldata',
      isRecv: false,
      reliable: true,
      broadcast: false,
    };
    const queue = new engine.Queue(qOpts, lib.appNetConn.conn);
    lib.appNetQueues.push(queue);

    await new Promise((resolve) => {
      let complete = 0;
      mgr.on(Events.Status, (status) => {
        if (status === MgrStatus.Ready) {
          complete++;
          if (complete >= 2) {
            return void resolve();
          }
        }
      });
      queue.on(Events.Status, (status) => {
        if (status === Status.Connected) {
          complete++;
          if (complete >= 2) {
            return void resolve();
          }
        }
      });
      queue.connect();
    });

    data1 = {
      dataId: '1',
      pub: now.toISOString(),
      expiresIn: 1000,
      networkAddr: 'addr1',
      data: '01',
    };
    await queue.sendMsg(Buffer.from(JSON.stringify(data1)));
    data2 = {
      dataId: '2',
      pub: new Date(now.getTime() + 1).toISOString(),
      expiresIn: 2000,
      networkAddr: 'addr2',
      data: '02',
      extension: { key: 'value' },
    };
    await queue.sendMsg(Buffer.from(JSON.stringify(data2)));
    data3 = {
      dataId: '3',
      pub: new Date(now.getTime() + 2).toISOString(),
      expiresIn: 3000,
      networkAddr: 'addr3',
      data: '03',
    };
    await queue.sendMsg(Buffer.from(JSON.stringify(data3)));

    const expectCount = engine === gmq.amqp ? 3 : 2;
    for (let i = 0; i < 100; i++) {
      if (testHandler.recvDlData.length === expectCount) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(
      testHandler.recvDlData.length,
      expectCount,
      `receive ${testHandler.recvDlData.length}/${expectCount} data`
    );

    for (let i = 0; i < expectCount; i++) {
      const data = testHandler.recvDlData.pop();
      assert.ok(data, `only receive ${i}/${expectCount} data`);
      const dataId = data.dataId;
      assert.ok(['1', '2', '3'].includes(dataId), `unexpected dataId ${dataId}`);
      if (dataId === '1') {
        assert.strictEqual(engine, gmq.amqp, 'data1 wrong engine');
        assert.strictEqual(
          data.pub.getTime(),
          now.getTime(),
          `data1.pub ${data.pub.toISOString()} eq ${now.toISOString()}`
        );
        assert.strictEqual(
          data.networkAddr,
          data1.networkAddr,
          `data1.networkAddr ${data.networkAddr} eq ${data1.networkAddr}`
        );
        assert.strictEqual(
          data.data.toString('hex'),
          data1.data,
          `data1.data ${data.data.toString('hex')} eq ${data1.data}`
        );
        assert.deepStrictEqual(
          data.extension,
          data1.extension,
          `data1.extension ${data.extension} eq ${data1.extension}`
        );
      } else if (dataId === '2') {
        assert.strictEqual(
          data.pub.getTime(),
          now.getTime() + 1,
          `data2.pub ${data.pub.toISOString()} eq ${new Date(now.getTime() + 1).toISOString()}`
        );
        assert.strictEqual(
          data.networkAddr,
          data2.networkAddr,
          `data2.networkAddr ${data.networkAddr} eq ${data2.networkAddr}`
        );
        assert.strictEqual(
          data.data.toString('hex'),
          data2.data,
          `data2.data ${data.data.toString('hex')} eq ${data2.data}`
        );
        assert.deepStrictEqual(
          data.extension,
          data2.extension,
          `data2.extension ${data.extension} eq ${data2.extension}`
        );
      } else if (dataId === '3') {
        assert.strictEqual(
          data.pub.getTime(),
          now.getTime() + 2,
          `data3.pub ${data.pub.toISOString()} eq ${new Date(now.getTime() + 2).toISOString()}`
        );
        assert.strictEqual(
          data.networkAddr,
          data3.networkAddr,
          `data3.networkAddr ${data.networkAddr} eq ${data3.networkAddr}`
        );
        assert.strictEqual(
          data.data.toString('hex'),
          data3.data,
          `data3.data ${data.data.toString('hex')} eq ${data3.data}`
        );
        assert.deepStrictEqual(
          data.extension,
          data3.extension,
          `data3.extension ${data.extension} eq ${data3.extension}`
        );
      }
    }
  };
}

/**
 * Test receiving dldata with wrong content.
 *
 * @param {Engine} engine
 */
function dldataWrong(engine) {
  return async function () {
    const testHandler = new TestHandler();

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
    const mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
    lib.netMgrs.push(mgr);

    lib.appNetConn = await lib.newConnection(engine);
    const qOpts = {
      name: 'broker.network.unit_code.code_network.dldata',
      isRecv: false,
      reliable: true,
      broadcast: false,
    };
    const queue = new engine.Queue(qOpts, lib.appNetConn.conn);
    lib.appNetQueues.push(queue);

    await new Promise((resolve) => {
      let complete = 0;
      mgr.on(Events.Status, (status) => {
        if (status === MgrStatus.Ready) {
          complete++;
          if (complete >= 2) {
            return void resolve();
          }
        }
      });
      queue.on(Events.Status, (status) => {
        if (status === Status.Connected) {
          complete++;
          if (complete >= 2) {
            return void resolve();
          }
        }
      });
      queue.connect();
    });

    await queue.sendMsg(Buffer.from('{'));
    await new Promise((resolve) => setTimeout(resolve, 1000));
    assert.ok(!testHandler.isDlDataRecv, 'should not receive data');
  };
}

/**
 * Test generating dldata-result.
 *
 * @param {Engine} engine
 */
function dldataResult(engine) {
  return async function () {
    const testHandler = new TestHandler();

    /** @type {NetDlDataResult} */
    let data1;
    /** @type {NetDlDataResult} */
    let data2;

    /** @type {Buffer[]} */
    const recvDldataResult = [];

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
    const mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
    lib.netMgrs.push(mgr);

    lib.appNetConn = await lib.newConnection(engine);
    const qOpts = {
      name: 'broker.network.unit_code.code_network.dldata-result',
      isRecv: true,
      reliable: true,
      broadcast: false,
      prefetch: 1,
      sharedPrefix: SHARED_PREFIX,
    };
    const queue = new engine.Queue(qOpts, lib.appNetConn.conn);
    lib.appNetQueues.push(queue);
    queue.setMsgHandler((queue, msg) => {
      recvDldataResult.push(msg.payload);
      queue.ack(msg, (_err) => {});
    });

    await new Promise((resolve) => {
      let complete = 0;
      mgr.on(Events.Status, (status) => {
        if (status === MgrStatus.Ready) {
          complete++;
          if (complete >= 2) {
            return void resolve();
          }
        }
      });
      queue.on(Events.Status, (status) => {
        if (status === Status.Connected) {
          complete++;
          if (complete >= 2) {
            return void resolve();
          }
        }
      });
      queue.connect();
    });

    data1 = {
      dataId: '1',
      status: -1,
    };
    await mgr.sendDlDataResult(data1);
    data2 = {
      dataId: '2',
      status: 1,
      message: 'error',
    };
    await mgr.sendDlDataResult(data2);

    for (let i = 0; i < 100; i++) {
      if (recvDldataResult.length === 2) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(
      recvDldataResult.length,
      2,
      `only receive ${recvDldataResult.length}/2 data`
    );

    for (let i = 0; i < 2; i++) {
      const dataBuff = recvDldataResult.pop();
      assert.ok(dataBuff, `gmq only receive ${i}/2 data`);
      const data = JSON.parse(dataBuff.toString());
      assert.ok(['1', '2'].includes(data.dataId), `receive wrong data ${data.dataId}`);
      if (data.dataId === '1') {
        assert.strictEqual(
          data.status,
          data1.status,
          `data1.status ${data.status} eq ${data1.status}`
        );
        assert.strictEqual(
          data.message,
          data1.message,
          `data1.message ${data.message} eq ${data1.message}`
        );
      } else if (data.dataId === '2') {
        assert.strictEqual(
          data.status,
          data2.status,
          `data2.status ${data.status} eq ${data2.status}`
        );
        assert.strictEqual(
          data.message,
          data2.message,
          `data2.message ${data.message} eq ${data2.message}`
        );
      }
    }
  };
}

/**
 * Test generating dldata-result with wrong content.
 *
 * @param {Engine} engine
 */
function dldataResultWrong(engine) {
  return async function () {
    const testHandler = new TestHandler();

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
    const mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
    lib.netMgrs.push(mgr);
    await new Promise((resolve) => {
      mgr.on(Events.Status, (status) => {
        if (status === MgrStatus.Ready) {
          return void resolve();
        }
      });
    });

    const shouldErrFn = () => {
      throw Error('should error');
    };
    const catchFn = (err) => assert.ok(!(err instanceof SdkError));

    mgr.sendDlDataResult(null).then(shouldErrFn).catch(catchFn);
    /** @type {NetDlDataResult} */
    const data = {};
    mgr.sendDlDataResult(data).then(shouldErrFn).catch(catchFn);
    data.dataId = '1';
    mgr.sendDlDataResult(data).then(shouldErrFn).catch(catchFn);
    data.status = 0;
    data.message = 1;
    mgr.sendDlDataResult(data).then(shouldErrFn).catch(catchFn);
  };
}

/**
 * Test receiving ctrl.
 *
 * @param {Engine} engine
 */
function ctrl(engine) {
  return async function () {
    const now = new Date();
    const testHandler = new TestHandler();

    let data1;
    let data2;
    let data3;
    let data4;
    let data5;
    let data6;

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
    const mgr = new NetworkMgr(connPool, hostUri, opts, handlers);
    lib.netMgrs.push(mgr);

    lib.appNetConn = await lib.newConnection(engine);
    const qOpts = {
      name: 'broker.network.unit_code.code_network.ctrl',
      isRecv: false,
      reliable: true,
      broadcast: false,
    };
    const queue = new engine.Queue(qOpts, lib.appNetConn.conn);
    lib.appNetQueues.push(queue);

    await new Promise((resolve) => {
      let complete = 0;
      mgr.on(Events.Status, (status) => {
        if (status === MgrStatus.Ready) {
          complete++;
          if (complete >= 2) {
            return void resolve();
          }
        }
      });
      queue.on(Events.Status, (status) => {
        if (status === Status.Connected) {
          complete++;
          if (complete >= 2) {
            return void resolve();
          }
        }
      });
      queue.connect();
    });

    data1 = {
      operation: 'add-device',
      time: now.toISOString(),
      new: { networkAddr: 'addr1' },
    };
    await queue.sendMsg(Buffer.from(JSON.stringify(data1)));
    data2 = {
      operation: 'add-device-bulk',
      time: new Date(now.getTime() + 1).toISOString(),
      new: { networkAddrs: ['addr2'] },
    };
    await queue.sendMsg(Buffer.from(JSON.stringify(data2)));
    data3 = {
      operation: 'add-device-range',
      time: new Date(now.getTime() + 2).toISOString(),
      new: {
        startAddr: '0001',
        endAddr: '0002',
      },
    };
    await queue.sendMsg(Buffer.from(JSON.stringify(data3)));
    data4 = {
      operation: 'del-device',
      time: new Date(now.getTime() + 3).toISOString(),
      new: { networkAddr: 'addr4' },
    };
    await queue.sendMsg(Buffer.from(JSON.stringify(data4)));
    data5 = {
      operation: 'del-device-bulk',
      time: new Date(now.getTime() + 4).toISOString(),
      new: { networkAddrs: ['addr5'] },
    };
    await queue.sendMsg(Buffer.from(JSON.stringify(data5)));
    data6 = {
      operation: 'del-device-range',
      time: new Date(now.getTime() + 5).toISOString(),
      new: {
        startAddr: '0003',
        endAddr: '0004',
      },
    };
    await queue.sendMsg(Buffer.from(JSON.stringify(data6)));

    const expectCount = engine === gmq.amqp ? 6 : 5;
    for (let i = 0; i < 100; i++) {
      if (testHandler.recvCtrl.length === expectCount) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(
      testHandler.recvCtrl.length,
      expectCount,
      `receive ${testHandler.recvCtrl.length}/${expectCount} data`
    );

    let recvDevAdd = false;
    let recvDevAddBulk = false;
    let recvDevAddRange = false;
    let recvDevDel = false;
    let recvDevDelBulk = false;
    let recvDevDelRange = false;
    for (let i = 0; i < expectCount; i++) {
      const data = testHandler.recvCtrl.pop();
      assert.ok(data, `only receive ${i}/${expectCount} data`);
      const operation = data.operation;
      if (operation === 'add-device') {
        assert.strictEqual(engine, gmq.amqp, 'data1 wrong engine');
        assert.strictEqual(
          data.time.getTime(),
          now.getTime(),
          `data1.time ${data.time.toISOString()} eq ${now.toISOString()}`
        );
        assert.deepStrictEqual(data.new, data1.new, `data1.new ${data.new} eq ${data1.new}`);
        recvDevAdd = true;
      } else if (operation === 'add-device-bulk') {
        assert.strictEqual(
          data.time.getTime(),
          now.getTime() + 1,
          `data2.time ${data.time.toISOString()} eq ${new Date(now.getTime() + 1).toISOString()}`
        );
        assert.deepStrictEqual(data.new, data2.new, `data2.new ${data.new} eq ${data2.new}`);
        recvDevAddBulk = true;
      } else if (operation === 'add-device-range') {
        assert.strictEqual(
          data.time.getTime(),
          now.getTime() + 2,
          `data3.time ${data.time.toISOString()} eq ${new Date(now.getTime() + 2).toISOString()}`
        );
        assert.deepStrictEqual(data.new, data3.new, `data3.new ${data.new} eq ${data3.new}`);
        recvDevAddRange = true;
      } else if (operation === 'del-device') {
        assert.strictEqual(
          data.time.getTime(),
          now.getTime() + 3,
          `data4.time ${data.time.toISOString()} eq ${new Date(now.getTime() + 3).toISOString()}`
        );
        assert.deepStrictEqual(data.new, data4.new, `data4.new ${data.new} eq ${data4.new}`);
        recvDevDel = true;
      } else if (operation === 'del-device-bulk') {
        assert.strictEqual(
          data.time.getTime(),
          now.getTime() + 4,
          `data5.time ${data.time.toISOString()} eq ${new Date(now.getTime() + 4).toISOString()}`
        );
        assert.deepStrictEqual(data.new, data5.new, `data5.new ${data.new} eq ${data5.new}`);
        recvDevDelBulk = true;
      } else if (operation === 'del-device-range') {
        assert.strictEqual(
          data.time.getTime(),
          now.getTime() + 5,
          `data6.time ${data.time.toISOString()} eq ${new Date(now.getTime() + 5).toISOString()}`
        );
        assert.deepStrictEqual(data.new, data6.new, `data6.new ${data.new} eq ${data6.new}`);
        recvDevDelRange = true;
      }
    }
    const result =
      (recvDevAdd || engine !== gmq.amqp) &&
      recvDevAddBulk &&
      recvDevAddRange &&
      recvDevDel &&
      recvDevDelBulk &&
      recvDevDelRange;
    assert.ok(result, 'not recv all');
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
