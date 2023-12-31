'use strict';

const { EventEmitter } = require('events');
const { URL } = require('url');

const async = require('async');

const { AmqpQueue } = require('general-mq/lib/amqp-queue');
const { DataTypes, Status, Events } = require('general-mq/lib/constants');
const { MqttQueue } = require('general-mq/lib/mqtt-queue');
const {
  Connection,
  DataMqStatus,
  CtrlAddDevice,
  CtrlAddDeviceBulk,
  CtrlAddDeviceRange,
  CtrlDelDevice,
  CtrlDelDeviceBulk,
  CtrlDelDeviceRange,
  Options,
  getConnection,
  newDataQueues,
  removeConnection,
} = require('./lib');
const { MgrStatus } = require('./constants');

/**
 * Uplink data from network to broker.
 *
 * @typedef {Object} NetUlData
 * @property {Date} time
 * @property {string} networkAddr
 * @property {Buffer} data
 * @property {Object} [extension]
 */

/**
 * Downlink data from broker to network.
 *
 * @typedef {Object} NetDlData
 * @property {string} dataId
 * @property {Date} pub
 * @property {number} expiresIn
 * @property {string} networkAddr
 * @property {Buffer} data
 * @property {Object} [extension]
 */

/**
 * Downlink data result when processing or completing data transfer to the device.
 *
 * @typedef {Object} NetDlDataResult
 * @property {string} dataId
 * @property {number} status
 * @property {string} [message]
 */

/**
 * Network control message from broker to network.
 *
 * @typedef {Object} NetCtrlMsg
 * @property {string} operation
 * @property {Date} time
 * @property {CtrlAddDevice|CtrlAddDeviceBulk|CtrlAddDeviceRange|CtrlDelDevice|CtrlDelDeviceBulk|CtrlDelDeviceRange} new
 */

/**
 * Connection status event.
 *
 * @event NetworkMgr#status
 * @type {Status}
 */

/**
 * @typedef {Object} NetMgrMsgHandlers
 * @property {OnNetDlData} onDlData
 * @property {OnNetCtrl} onCtrl
 */

/**
 * @callback OnNetDlData
 * @param {NetworkMgr} mgr
 * @param {NetDlData} data
 * @param {function} callback
 *   @param {?Error} callback.err Use error to NACK the message.
 */

/**
 * @callback OnNetCtrl
 * @param {NetworkMgr} mgr
 * @param {NetCtrlMsg} data
 * @param {function} callback
 *   @param {?Error} callback.err Use error to NACK the message.
 */

/**
 * The manager for network queues.
 *
 * @class NetworkMgr
 * @fires NetworkMgr#status
 */
class NetworkMgr extends EventEmitter {
  /**
   * @param {Map<string, Connection>} connPool
   * @param {URL} hostUri
   * @param {Options} opts
   * @param {NetMgrMsgHandlers} handler
   * @throws {Error} Wrong arguments.
   */
  constructor(connPool, hostUri, opts, handler) {
    super();

    if (!connPool || !(connPool instanceof Map)) {
      throw Error('`connPool` is not a Connection pool map');
    } else if (!hostUri || !(hostUri instanceof URL) || !URL.canParse(hostUri.toString())) {
      throw Error('`hostUri` is not a valid URI');
    } else if (!opts || typeof opts !== DataTypes.Object || Array.isArray(opts)) {
      throw Error('`opts` is not an object');
    } else if (!handler || typeof handler !== DataTypes.Object || Array.isArray(handler)) {
      throw Error('`handler` is not an object');
    } else if (
      typeof handler.onDlData !== DataTypes.Function ||
      typeof handler.onCtrl !== DataTypes.Function
    ) {
      throw Error('`onDlData` and `onCtrl` must be functions');
    }

    const conn = getConnection(connPool, hostUri);
    const queues = newDataQueues(conn, opts, QUEUE_PREFIX, true);
    conn.conn.connect();

    this.#opts = { ...opts };
    this.#connPool = connPool;
    this.#hostUri = hostUri.toString();
    this.#uldata = queues.uldata;
    this.#dldata = queues.dldata;
    this.#dldataResult = queues.dldataResult;
    this.#ctrl = queues.ctrl;
    this.#status = MgrStatus.NotReady;
    this.#mgrMsgHandler = handler;

    this.#uldata.on(Events.Status, this.#gmqStatusHandler.bind(this));
    this.#uldata.setMsgHandler(this.#gmqMsgHandler.bind(this));
    this.#uldata.connect();

    this.#dldata.on(Events.Status, this.#gmqStatusHandler.bind(this));
    this.#dldata.setMsgHandler(this.#gmqMsgHandler.bind(this));
    this.#dldata.connect();

    this.#dldataResult.on(Events.Status, this.#gmqStatusHandler.bind(this));
    this.#dldataResult.setMsgHandler(this.#gmqMsgHandler.bind(this));
    this.#dldataResult.connect();

    this.#ctrl.on(Events.Status, this.#gmqStatusHandler.bind(this));
    this.#ctrl.setMsgHandler(this.#gmqMsgHandler.bind(this));
    this.#ctrl.connect();

    conn.count += 4;
  }

  /**
   * @returns {string} The associated unit ID of the network.
   */
  unitId() {
    return this.#opts.unitId;
  }

  /**
   * @returns {string} The associated unit code of the network.
   */
  unitCode() {
    return this.#opts.unitCode;
  }

  /**
   * @returns {string} The network ID.
   */
  id() {
    return this.#opts.id;
  }

  /**
   * @returns {string} The network code.
   */
  name() {
    return this.#opts.name;
  }

  /**
   * @returns {MgrStatus} Manager status.
   */
  status() {
    return this.#status;
  }

  /**
   * @returns {DataMqStatus} Manager status.
   */
  mqStatus() {
    return {
      uldata: this.#uldata.status(),
      dldata: this.#dldata.status(),
      dldataResp: Status.Closed,
      dldataResult: this.#dldataResult.status(),
      ctrl: this.#ctrl.status(),
    };
  }

  /**
   * To close the manager queues.
   * The underlying connection will be closed when there are no queues use it.
   *
   * @param {function} callback
   *   @param {?Error} callback.err
   */
  close(callback) {
    const self = this;

    async.waterfall(
      [
        function (cb) {
          self.#uldata.removeAllListeners();
          self.#uldata.close((err) => {
            cb(err || null);
          });
        },
        function (cb) {
          self.#dldata.removeAllListeners();
          self.#dldata.close((err) => {
            cb(err || null);
          });
        },
        function (cb) {
          self.#dldataResult.removeAllListeners();
          self.#dldataResult.close((err) => {
            cb(err || null);
          });
        },
        function (cb) {
          self.#ctrl.removeAllListeners();
          self.#ctrl.close((err) => {
            cb(err || null);
          });
        },
        function (cb) {
          removeConnection(self.#connPool, self.#hostUri, 4, (err) => {
            cb(err || null);
          });
        },
      ],
      callback
    );
  }

  /**
   * Send uplink data to the broker.
   *
   * @param {NetUlData} data
   * @param {function} callback
   *   @param {?Error} callback.err
   * @throws {Error} Wrong arguments.
   */
  sendUlData(data, callback) {
    if (!data || typeof data !== DataTypes.Object || Array.isArray(data)) {
      throw Error('`data` is not an object');
    } else if (!data.time || !(data.time instanceof Date) || isNaN(data.time.getTime())) {
      throw Error('`data.time` is not a Date');
    } else if (!data.networkAddr || typeof data.networkAddr !== DataTypes.String) {
      throw Error('`data.networkAddr` is not a string');
    } else if (!(data.data instanceof Buffer)) {
      throw Error('`data.data` is not a Buffer');
    } else if (
      data.extension !== undefined &&
      (!data.extension ||
        typeof data.extension !== DataTypes.Object ||
        Array.isArray(data.extension))
    ) {
      throw Error('`data.extension` is not an object');
    }

    const uldata = {
      time: data.time.toISOString(),
      networkAddr: data.networkAddr,
      data: data.data.toString('hex'),
      extension: data.extension || undefined,
    };
    const payload = Buffer.from(JSON.stringify(uldata));
    this.#uldata.sendMsg(payload, (err) => {
      callback(err || null);
    });
  }

  /**
   * Send downlink result data to the broker.
   *
   * @param {NetDlDataResult} data
   * @param {function} callback
   *   @param {?Error} callback.err
   * @throws {Error} Wrong arguments.
   */
  sendDlDataResult(data, callback) {
    if (!data || typeof data !== DataTypes.Object || Array.isArray(data)) {
      throw Error('`data` is not an object');
    } else if (!data.dataId || typeof data.dataId !== DataTypes.String) {
      throw Error('`data.dataId` is not a string');
    } else if (!Number.isInteger(data.status)) {
      throw Error('`data.status` is not an integer');
    } else if (data.message !== undefined && typeof data.message !== DataTypes.String) {
      throw Error('`data.message` is not a string');
    }

    const dldataResult = {
      dataId: data.dataId,
      status: data.status,
      message: data.message || undefined,
    };
    const payload = Buffer.from(JSON.stringify(dldataResult));
    this.#dldataResult.sendMsg(payload, (err) => {
      callback(err || null);
    });
  }

  /**
   * The handler for the gmq.Queue#status events.
   */
  #gmqStatusHandler(_queue, _status) {
    let status;
    if (
      this.#uldata.status() === Status.Connected &&
      this.#dldata.status() === Status.Connected &&
      this.#dldataResult.status() === Status.Connected &&
      this.#ctrl.status() === Status.Connected
    ) {
      status = MgrStatus.Ready;
    } else {
      status = MgrStatus.NotReady;
    }

    if (this.#status === status) {
      return;
    }
    this.#status = status;
    this.emit(Events.Status, status);
  }

  /**
   * The message handler for the gmq.Queue.
   */
  #gmqMsgHandler(queue, msg) {
    let data;
    try {
      data = JSON.parse(msg.payload.toString());
    } catch (e) {
      queue.ack(msg, (_err) => {});
      return;
    }

    const self = this;
    if (queue.name() === this.#dldata.name()) {
      data.pub = new Date(data.pub);
      data.data = Buffer.from(data.data, 'hex');
      this.#mgrMsgHandler.onDlData(this, data, (err) => {
        if (err) {
          self.#dldata.nack(msg, (_err) => {});
        } else {
          self.#dldata.ack(msg, (_err) => {});
        }
      });
    } else if (queue.name() === this.#ctrl.name()) {
      data.time = new Date(data.time);
      this.#mgrMsgHandler.onCtrl(this, data, (err) => {
        if (err) {
          self.#ctrl.nack(msg, (_err) => {});
        } else {
          self.#ctrl.ack(msg, (_err) => {});
        }
      });
    } else {
      return;
    }
  }

  /** @type {Options} */
  #opts;
  /**
   * Information for delete connection automatically.
   *
   * @type {Map<string, Connection>}
   */
  #connPool;
  /**
   * Information for delete connection automatically.
   *
   * @type {string}
   */
  #hostUri;
  /** @type {AmqpQueue|MqttQueue} */
  #uldata;
  /** @type {AmqpQueue|MqttQueue} */
  #dldata;
  /** @type {AmqpQueue|MqttQueue} */
  #dldataResult;
  /** @type {AmqpQueue|MqttQueue} */
  #ctrl;
  /** @type {Status} */
  #status;
  /** @type {NetMgrMsgHandlers} */
  #mgrMsgHandler;
}

const QUEUE_PREFIX = 'broker.network';

module.exports = {
  NetworkMgr,
};
