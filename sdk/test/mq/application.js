'use strict';

const assert = require('assert');

const gmq = require('general-mq');
const { AmqpConnection, AmqpQueue, MqttConnection, MqttQueue } = gmq;
const { Events, Status } = gmq.constants;

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
const mqSdkLib = require('../../mq/lib');
const { MgrStatus } = require('../../mq/constants');

const lib = require('./lib');
const { connHostUri, SHARED_PREFIX } = lib;

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
  return async function () {
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
    lib.appMgrs.push(mgr);

    assert.strictEqual(mgr.unitId(), 'unit_id');
    assert.strictEqual(mgr.unitCode(), 'unit_code');
    assert.strictEqual(mgr.id(), 'id_application');
    assert.strictEqual(mgr.name(), 'code_application');
    assert.strictEqual(mgr.status(), MgrStatus.NotReady);

    await new Promise((resolve) => {
      mgr.on(Events.Status, (status) => {
        if (status === MgrStatus.Ready) {
          const mgrStatus = mgr.status();
          assert.strictEqual(mgrStatus, MgrStatus.Ready, 'manager not ready');
          const qStatus = mgr.mqStatus();
          assert.strictEqual(qStatus.uldata, Status.Connected, 'uldata not connected');
          assert.strictEqual(qStatus.dldata, Status.Connected, 'dldata not connected');
          assert.strictEqual(qStatus.dldataResp, Status.Connected, 'dldataResp not connected');
          assert.strictEqual(qStatus.dldataResult, Status.Connected, 'dldataResult not connected');
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
      onUlData: () => {},
      onDlDataResp: () => {},
      onDlDataResult: () => {},
    };

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
    lib.appMgrs.push(mgr2);

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
      onUlData: () => {},
      onDlDataResp: () => {},
      onDlDataResult: () => {},
    };

    assert.throws(() => {
      new ApplicationMgr({});
    });
    assert.throws(() => new ApplicationMgr(connPool, 'url'));
    assert.throws(() => new ApplicationMgr(connPool, hostUri, null));
    assert.throws(() => new ApplicationMgr(connPool, hostUri, { unitId: '' }));
    assert.throws(() => new ApplicationMgr(connPool, hostUri, { unitId: 'unit_id' }, null));
    assert.throws(() => new ApplicationMgr(connPool, hostUri, { unitId: 'unit_id' }, {}));
    assert.throws(
      () =>
        new ApplicationMgr(connPool, new URL('tcp://localhost'), { unitId: 'unit_id' }, handlers)
    );
    const opts = {
      unitId: 1,
    };
    assert.throws(() => new ApplicationMgr(connPool, hostUri, opts, handlers));
    opts.unitId = 'unit_id';
    opts.unitCode = 1;
    assert.throws(() => new ApplicationMgr(connPool, hostUri, opts, handlers));
    opts.unitCode = 'unit_code';
    opts.id = '';
    assert.throws(() => new ApplicationMgr(connPool, hostUri, opts, handlers));
    opts.id = 'id';
    opts.name = '';
    assert.throws(() => new ApplicationMgr(connPool, hostUri, opts, handlers));
    opts.name = 'code';
    opts.unitCode = '';
    assert.throws(() => new ApplicationMgr(connPool, hostUri, opts, handlers));
    opts.unitCode = 'unit_code';
    opts.prefetch = 65536;
    assert.throws(() => new ApplicationMgr(connPool, hostUri, opts, handlers));
    delete opts.prefetch;
    opts.persistent = 0;
    assert.throws(() => new ApplicationMgr(connPool, hostUri, opts, handlers));
    delete opts.persistent;
    opts.sharedPrefix = null;
    assert.throws(() => new ApplicationMgr(connPool, hostUri, opts, handlers));
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
 * Test receiving uldata.
 *
 * @param {Engine} engine
 */
function uldata(engine) {
  return async function () {
    const now = new Date();
    const testHandler = new TestHandler();

    /** @type {AppUlData} */
    let data1;
    /** @type {AppUlData} */
    let data2;
    /** @type {AppUlData} */
    let data3;

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
    const mgr = new ApplicationMgr(connPool, hostUri, opts, handlers);
    lib.appMgrs.push(mgr);

    lib.appNetConn = await lib.newConnection(engine);
    const qOpts = {
      name: 'broker.application.unit_code.code_application.uldata',
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
      time: now.toISOString(),
      pub: new Date(now.getTime() + 1).toISOString(),
      deviceId: 'device_id1',
      networkId: 'network_id1',
      networkCode: 'network_code1',
      networkAddr: 'network_addr1',
      isPublic: true,
      data: '01',
    };
    await queue.sendMsg(Buffer.from(JSON.stringify(data1)));
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
    await queue.sendMsg(Buffer.from(JSON.stringify(data2)));
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
    await queue.sendMsg(Buffer.from(JSON.stringify(data3)));

    const expectCount = engine === gmq.amqp ? 3 : 2;
    for (let i = 0; i < 100; i++) {
      if (testHandler.recvUlData.length === expectCount) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(
      testHandler.recvUlData.length,
      expectCount,
      `receive ${testHandler.recvUlData.length}/${expectCount} data`
    );

    for (let i = 0; i < expectCount; i++) {
      const data = testHandler.recvUlData.pop();
      assert.ok(data, `only receive ${i}/${expectCount} data`);
      const dataId = data.dataId;
      assert.ok(['1', '2', '3'].includes(dataId), `unexpected dataId ${dataId}`);
      if (dataId === '1') {
        assert.strictEqual(engine, gmq.amqp, 'data1 wrong engine');
        assert.strictEqual(
          data.time.getTime(),
          now.getTime(),
          `data1.time ${data.time.toISOString()} eq ${now}.toISOString()`
        );
        assert.strictEqual(
          data.pub.getTime(),
          now.getTime() + 1,
          `data1.pub ${data.pub.toISOString()} eq ${new Date(now.getTime() + 1).toISOString()}`
        );
        assert.strictEqual(
          data.deviceId,
          data1.deviceId,
          `data1.deviceId ${data.deviceId} eq ${data1.deviceId}`
        );
        assert.strictEqual(
          data.networkId,
          data1.networkId,
          `data1.networkId ${data.networkId} eq ${data1.networkId}`
        );
        assert.strictEqual(
          data.networkCode,
          data1.networkCode,
          `data1.networkCode ${data.networkCode} eq ${data1.networkCode}`
        );
        assert.strictEqual(
          data.networkAddr,
          data1.networkAddr,
          `data1.networkAddr ${data.networkAddr} eq ${data1.networkAddr}`
        );
        assert.strictEqual(
          data.isPublic,
          data1.isPublic,
          `data1.isPublic ${data.isPublic} eq ${data1.isPublic}`
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
          data.time.getTime(),
          now.getTime() + 1,
          `data2.time ${data.time.toISOString()} eq ${new Date(now.getTime() + 1).toISOString()}`
        );
        assert.strictEqual(
          data.pub.getTime(),
          now.getTime() + 2,
          `data2.pub ${data.pub.toISOString()} eq ${new Date(now.getTime() + 2).toISOString()}`
        );
        assert.strictEqual(
          data.deviceId,
          data2.deviceId,
          `data2.deviceId ${data.deviceId} eq ${data2.deviceId}`
        );
        assert.strictEqual(
          data.networkId,
          data2.networkId,
          `data2.networkId ${data.networkId} eq ${data2.networkId}`
        );
        assert.strictEqual(
          data.networkCode,
          data2.networkCode,
          `data2.networkCode ${data.networkCode} eq ${data2.networkCode}`
        );
        assert.strictEqual(
          data.networkAddr,
          data2.networkAddr,
          `data2.networkAddr ${data.networkAddr} eq ${data2.networkAddr}`
        );
        assert.strictEqual(
          data.isPublic,
          data2.isPublic,
          `data2.isPublic ${data.isPublic} eq ${data2.isPublic}`
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
          data.time.getTime(),
          now.getTime() + 2,
          `data3.time ${data.time.toISOString()} eq ${new Date(now.getTime() + 2).toISOString()}`
        );
        assert.strictEqual(
          data.pub.getTime(),
          now.getTime() + 3,
          `data3.pub ${data.pub.toISOString()} eq ${new Date(now.getTime() + 3).toISOString()}`
        );
        assert.strictEqual(
          data.deviceId,
          data3.deviceId,
          `data3.deviceId ${data.deviceId} eq ${data3.deviceId}`
        );
        assert.strictEqual(
          data.networkId,
          data3.networkId,
          `data3.networkId ${data.networkId} eq ${data3.networkId}`
        );
        assert.strictEqual(
          data.networkCode,
          data3.networkCode,
          `data3.networkCode ${data.networkCode} eq ${data3.networkCode}`
        );
        assert.strictEqual(
          data.networkAddr,
          data3.networkAddr,
          `data3.networkAddr ${data.networkAddr} eq ${data3.networkAddr}`
        );
        assert.strictEqual(
          data.isPublic,
          data3.isPublic,
          `data3.isPublic ${data.isPublic} eq ${data3.isPublic}`
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
 * Test receiving uldata with wrong content.
 *
 * @param {Engine} engine
 */
function uldataWrong(engine) {
  return async function () {
    const testHandler = new TestHandler();

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
    const mgr = new ApplicationMgr(connPool, hostUri, opts, handlers);
    lib.appMgrs.push(mgr);

    lib.appNetConn = await lib.newConnection(engine);
    const qOpts = {
      name: 'broker.application.unit_code.code_application.uldata',
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
    assert.ok(!testHandler.isUlDataRecv, 'should not receive data');
  };
}

/**
 * Test generating dldata.
 *
 * @param {Engine} engine
 */
function dldata(engine) {
  return async function () {
    const testHandler = new TestHandler();

    /** @type {AppDlData} */
    let data1;
    /** @type {AppDlData} */
    let data2;

    /** @type {Buffer[]} */
    const recvDldata = [];

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
    const mgr = new ApplicationMgr(connPool, hostUri, opts, handlers);
    lib.appMgrs.push(mgr);

    lib.appNetConn = await lib.newConnection(engine);
    const qOpts = {
      name: 'broker.application.unit_code.code_application.dldata',
      isRecv: true,
      reliable: true,
      broadcast: false,
      prefetch: 1,
      sharedPrefix: SHARED_PREFIX,
    };
    const queue = new engine.Queue(qOpts, lib.appNetConn.conn);
    lib.appNetQueues.push(queue);
    queue.setMsgHandler((queue, msg) => {
      recvDldata.push(msg.payload);
      queue.ack(msg);
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
      correlationId: '1',
      deviceId: 'device1',
      data: Buffer.from('01', 'hex'),
      extension: { key: 'value' },
    };
    await mgr.sendDlData(data1);
    data2 = {
      correlationId: '2',
      networkCode: 'code',
      networkAddr: 'addr2',
      data: Buffer.from('02', 'hex'),
    };
    await mgr.sendDlData(data2);

    for (let i = 0; i < 100; i++) {
      if (recvDldata.length === 2) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(recvDldata.length, 2, `only receive ${recvDldata.length}/2 data`);

    for (let i = 0; i < 2; i++) {
      const dataBuff = recvDldata.pop();
      assert.ok(dataBuff, `gmq only receive ${i}/2 data`);
      const data = JSON.parse(dataBuff.toString());
      assert.ok(
        ['1', '2'].includes(data.correlationId),
        `unexpected correlationId ${data.correlationId}`
      );
      if (data.correlationId === '1') {
        assert.strictEqual(
          data.deviceId,
          data1.deviceId,
          `data1.deviceId ${data.deviceId} eq ${data1.deviceId}`
        );
        assert.strictEqual(
          data.networkCode,
          undefined,
          `data1.networkCode ${data.networkCode} eq ${data1.networkCode}`
        );
        assert.strictEqual(
          data.networkAddr,
          undefined,
          `data1.networkAddr ${data.networkAddr} eq ${data1.networkAddr}`
        );
        assert.strictEqual(
          data.data,
          data1.data.toString('hex'),
          `data1.deviceId ${data.data} eq ${data1.data.toString('hex')}`
        );
        assert.deepStrictEqual(
          data.extension,
          data1.extension,
          `data1.extension ${data.extension} eq ${data1.extension}`
        );
      } else if (data.correlationId === '2') {
        assert.strictEqual(
          data.deviceId,
          undefined,
          `data2.deviceId ${data.deviceId} eq ${data2.deviceId}`
        );
        assert.strictEqual(
          data.networkCode,
          data2.networkCode,
          `data2.networkCode ${data.networkCode} eq ${data2.networkCode}`
        );
        assert.strictEqual(
          data.networkAddr,
          data2.networkAddr,
          `data2.networkAddr ${data.networkAddr} eq ${data2.networkAddr}`
        );
        assert.strictEqual(
          data.data,
          data2.data.toString('hex'),
          `data2.deviceId ${data.data} eq ${data2.data.toString('hex')}`
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
 * Test sending dldata with wrong content.
 *
 * @param {Engine} engine
 */
function dldataWrong(engine) {
  return async function () {
    const testHandler = new TestHandler();

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
    const mgr = new ApplicationMgr(connPool, hostUri, opts, handlers);
    lib.appMgrs.push(mgr);
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

    mgr.sendDlData(null).then(shouldErrFn).catch(catchFn);
    /** @type {AppDlData} */
    const data = {
      correlationId: 1,
    };
    mgr.sendDlData(data).then(shouldErrFn).catch(catchFn);
    data.correlationId = '1';
    data.deviceId = 1;
    mgr.sendDlData(data).then(shouldErrFn).catch(catchFn);
    delete data.deviceId;
    data.networkCode = 1;
    mgr.sendDlData(data).then(shouldErrFn).catch(catchFn);
    data.networkCode = 'code';
    data.networkAddr = 1;
    mgr.sendDlData(data).then(shouldErrFn).catch(catchFn);
    data.networkAddr = 'addr';
    data.data = [1];
    mgr.sendDlData(data).then(shouldErrFn).catch(catchFn);
    data.data = Buffer.from('01', 'hex');
    data.extension = [];
    mgr.sendDlData(data).then(shouldErrFn).catch(catchFn);
    delete data.networkAddr;
    delete data.extension;
    mgr.sendDlData(data).then(shouldErrFn).catch(catchFn);
    delete data.networkCode;
    data.networkAddr = 'addr';
    mgr.sendDlData(data).then(shouldErrFn).catch(catchFn);
  };
}

/**
 * Test receiving dldata-resp.
 *
 * @param {Engine} engine
 */
function dldataResp(engine) {
  return async function () {
    const testHandler = new TestHandler();

    /** @type {AppDlDataResp} */
    let data1;
    /** @type {AppDlDataResp} */
    let data2;
    /** @type {AppDlDataResp} */
    let data3;

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
    const mgr = new ApplicationMgr(connPool, hostUri, opts, handlers);
    lib.appMgrs.push(mgr);

    lib.appNetConn = await lib.newConnection(engine);
    const qOpts = {
      name: 'broker.application.unit_code.code_application.dldata-resp',
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
            return void cb();
          }
        }
      });
      queue.connect();
    });

    data1 = {
      correlationId: '1',
      dataId: 'data_id1',
    };
    await queue.sendMsg(Buffer.from(JSON.stringify(data1)));
    data2 = {
      correlationId: '2',
      dataId: 'data_id2',
    };
    await queue.sendMsg(Buffer.from(JSON.stringify(data2)));
    data3 = {
      correlationId: '3',
      error: 'error3',
      message: 'message3',
    };
    await queue.sendMsg(Buffer.from(JSON.stringify(data3)));

    const expectCount = engine === gmq.amqp ? 3 : 2;
    for (let i = 0; i < 100; i++) {
      if (testHandler.recvDlDataResp.length === expectCount) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    for (let i = 0; i < expectCount; i++) {
      const data = testHandler.recvDlDataResp.pop();
      assert.ok(data, `only receive ${i}/${expectCount} data`);
      const correlationId = data.correlationId;
      assert.ok(
        ['1', '2', '3'].includes(correlationId),
        `unexpected correlationId ${correlationId}`
      );
      if (correlationId === '1') {
        assert.strictEqual(engine, gmq.amqp, 'data1 wrong engine');
        assert.strictEqual(
          data.dataId,
          data1.dataId,
          `data1.dataId ${data.dataId} eq ${data1.dataId}`
        );
        assert.strictEqual(data.error, data1.error, `data1.error ${data.error} eq ${data1.error}`);
        assert.strictEqual(
          data.message,
          data1.message,
          `data1.message ${data.message} eq ${data1.message}`
        );
      } else if (correlationId === '2') {
        assert.strictEqual(
          data.dataId,
          data2.dataId,
          `data2.dataId ${data.dataId} eq ${data2.dataId}`
        );
        assert.strictEqual(data.error, data2.error, `data2.error ${data.error} eq ${data2.error}`);
        assert.strictEqual(
          data.message,
          data2.message,
          `data1.message ${data.message} eq ${data2.message}`
        );
      } else if (correlationId === '3') {
        assert.strictEqual(
          data.dataId,
          data3.dataId,
          `data3.dataId ${data.dataId} eq ${data3.dataId}`
        );
        assert.strictEqual(data.error, data3.error, `data3.error ${data.error} eq ${data3.error}`);
        assert.strictEqual(
          data.message,
          data3.message,
          `data1.message ${data.message} eq ${data3.message}`
        );
      }
    }
  };
}

/**
 * Test receiving dldata-result.
 *
 * @param {Engine} engine
 */
function dldataResult(engine) {
  return async function () {
    const testHandler = new TestHandler();

    /** @type {AppDlDataResult} */
    let data1;
    /** @type {AppDlDataResult} */
    let data2;
    /** @type {AppDlDataResult} */
    let data3;

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
    const mgr = new ApplicationMgr(connPool, hostUri, opts, handlers);
    lib.appMgrs.push(mgr);

    lib.appNetConn = await lib.newConnection(engine);
    const qOpts = {
      name: 'broker.application.unit_code.code_application.dldata-result',
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
      status: -1,
    };
    await queue.sendMsg(Buffer.from(JSON.stringify(data1)));
    data2 = {
      dataId: '2',
      status: 0,
    };
    await queue.sendMsg(Buffer.from(JSON.stringify(data2)));
    data3 = {
      dataId: '3',
      status: 1,
      message: 'error',
    };
    await queue.sendMsg(Buffer.from(JSON.stringify(data3)));

    const expectCount = engine === gmq.amqp ? 3 : 2;
    for (let i = 0; i < 100; i++) {
      if (testHandler.recvDlDataResult.length === expectCount) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    for (let i = 0; i < expectCount; i++) {
      const data = testHandler.recvDlDataResult.pop();
      assert.ok(data, `only receive ${i}/${expectCount} data`);
      const dataId = data.dataId;
      assert.ok(['1', '2', '3'].includes(dataId), `unexpected dataId ${dataId}`);
      if (dataId === '1') {
        assert.strictEqual(engine, gmq.amqp, 'data1 wrong engine');
        assert.strictEqual(
          data.status,
          data1.status,
          `data1.status ${data.status} eq ${data1.error}`
        );
        assert.strictEqual(
          data.message,
          data1.message,
          `data1.message ${data.message} eq ${data1.message}`
        );
      } else if (dataId === '2') {
        assert.strictEqual(
          data2.status,
          data2.status,
          `data2.status ${data.status} eq ${data2.error}`
        );
        assert.strictEqual(
          data.message,
          data2.message,
          `data2.message ${data.message} eq ${data2.message}`
        );
      } else if (dataId === '3') {
        assert.strictEqual(
          data3.status,
          data3.status,
          `data3.status ${data.status} eq ${data3.error}`
        );
        assert.strictEqual(
          data.message,
          data3.message,
          `data3.message ${data.message} eq ${data3.message}`
        );
      }
    }
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
