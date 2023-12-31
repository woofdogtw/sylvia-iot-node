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
  Options,
  getConnection,
  removeConnection,
  newDataQueues,
} = require('./lib');
const { MgrStatus, Errors } = require('./constants');

/**
 * Uplink data from broker to application.
 *
 * @typedef {Object} AppUlData
 * @property {string} dataId
 * @property {Date} time
 * @property {Date} pub
 * @property {string} deviceId
 * @property {string} networkId
 * @property {string} networkCode
 * @property {string} networkAddr
 * @property {boolean} isPublic
 * @property {Buffer} data
 * @property {Object} [extension]
 */

/**
 * Downlink data from application to broker.
 *
 * @typedef {Object} AppDlData
 * @property {string} correlationId
 * @property {string} [deviceId]
 * @property {string} [networkCode]
 * @property {string} [networkAddr]
 * @property {Buffer} data
 * @property {Object} [extension]
 */

/**
 * Downlink data response for `AppDlData`.
 *
 * @typedef {Object} AppDlDataResp
 * @property {string} correlationId
 * @property {string} [dataId]
 * @property {string} [error]
 * @property {string} [message]
 */

/**
 * Downlink data result when processing or completing data transfer to the device.
 *
 * @typedef {Object} AppDlDataResult
 * @property {string} dataId
 * @property {number} status
 * @property {string} [message]
 */

/**
 * Connection status event.
 *
 * @event ApplicationMgr#status
 * @type {Status}
 */

/**
 * @typedef {Object} AppMgrMsgHandlers
 * @property {OnAppUlData} onUlData
 * @property {OnAppDlDataResp} onDlDataResp
 * @property {OnAppDlDataResult} onDlDataResult
 */

/**
 * @callback OnAppUlData
 * @param {ApplicationMgr} mgr
 * @param {AppUlData} data
 * @param {function} callback
 *   @param {?Error} callback.err Use error to NACK the message.
 */

/**
 * @callback OnAppDlDataResp
 * @param {ApplicationMgr} mgr
 * @param {AppDlDataResp} data
 * @param {function} callback
 *   @param {?Error} callback.err Use error to NACK the message.
 */

/**
 * @callback OnAppDlDataResult
 * @param {ApplicationMgr} mgr
 * @param {AppDlDataResult} data
 * @param {function} callback
 *   @param {?Error} callback.err Use error to NACK the message.
 */

/**
 * The manager for application queues.
 *
 * @class ApplicationMgr
 * @fires ApplicationMgr#status
 */
class ApplicationMgr extends EventEmitter {
  /**
   * @param {Map<string, Connection>} connPool
   * @param {URL} hostUri
   * @param {Options} opts
   * @param {AppMgrMsgHandlers} handler
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
    } else if (!opts.unitId) {
      throw Error('`opts.unitId` cannot be empty for application');
    } else if (!handler || typeof handler !== DataTypes.Object || Array.isArray(handler)) {
      throw Error('`handler` is not an object');
    } else if (
      typeof handler.onUlData !== DataTypes.Function ||
      typeof handler.onDlDataResp !== DataTypes.Function ||
      typeof handler.onDlDataResult !== DataTypes.Function
    ) {
      throw Error('`onUlData`, `onDlDataResp` and `onDlDataResult` must be functions');
    }

    const conn = getConnection(connPool, hostUri);
    const queues = newDataQueues(conn, opts, QUEUE_PREFIX, false);
    conn.conn.connect();

    this.#opts = { ...opts };
    this.#connPool = connPool;
    this.#hostUri = hostUri.toString();
    this.#uldata = queues.uldata;
    this.#dldata = queues.dldata;
    this.#dldataResp = queues.dldataResp;
    this.#dldataResult = queues.dldataResult;
    this.#status = MgrStatus.NotReady;
    this.#mgrMsgHandler = handler;

    this.#uldata.on(Events.Status, this.#gmqStatusHandler.bind(this));
    this.#uldata.setMsgHandler(this.#gmqMsgHandler.bind(this));
    this.#uldata.connect();

    this.#dldata.on(Events.Status, this.#gmqStatusHandler.bind(this));
    this.#dldata.setMsgHandler(this.#gmqMsgHandler.bind(this));
    this.#dldata.connect();

    this.#dldataResp.on(Events.Status, this.#gmqStatusHandler.bind(this));
    this.#dldataResp.setMsgHandler(this.#gmqMsgHandler.bind(this));
    this.#dldataResp.connect();

    this.#dldataResult.on(Events.Status, this.#gmqStatusHandler.bind(this));
    this.#dldataResult.setMsgHandler(this.#gmqMsgHandler.bind(this));
    this.#dldataResult.connect();

    conn.count += 4;
  }

  /**
   * @returns {string} The associated unit ID of the application.
   */
  unitId() {
    return this.#opts.unitId;
  }

  /**
   * @returns {string} The associated unit code of the application.
   */
  unitCode() {
    return this.#opts.unitCode;
  }

  /**
   * @returns {string} The application ID.
   */
  id() {
    return this.#opts.id;
  }

  /**
   * @returns {string} The application code.
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
      dldataResp: this.#dldataResp.status(),
      dldataResult: this.#dldataResult.status(),
      ctrl: Status.Closed,
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
          self.#dldataResp.removeAllListeners();
          self.#dldataResp.close((err) => {
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
          removeConnection(self.#connPool, self.#hostUri, 4, (err) => {
            cb(err || null);
          });
        },
      ],
      callback
    );
  }

  /**
   * Send downlink data `AppDlData` to the broker.
   *
   * @param {AppDlData} data
   * @param {function} callback
   *   @param {?Error} callback.err
   * @throws {Error} Wrong arguments.
   */
  sendDlData(data, callback) {
    if (!data || typeof data !== DataTypes.Object || Array.isArray(data)) {
      throw Error('`data` is not an object');
    } else if (!data.correlationId || typeof data.correlationId !== DataTypes.String) {
      throw Error(ErrParamCorrId);
    } else if (data.deviceId !== undefined && typeof data.deviceId !== DataTypes.String) {
      throw Error('`data.deviceId` is not a string');
    } else if (data.networkCode !== undefined && typeof data.networkCode !== DataTypes.String) {
      throw Error('`data.networkCode` is not a string');
    } else if (data.networkAddr !== undefined && typeof data.networkAddr !== DataTypes.String) {
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
    if (!data.deviceId) {
      if ((data.networkCode && !data.networkAddr) || (!data.networkCode && data.networkAddr)) {
        throw Error(Errors.ErrParamDev);
      }
    }

    const dldata = {
      correlationId: data.correlationId,
      deviceId: data.deviceId || undefined,
      networkCode: data.networkCode || undefined,
      networkAddr: data.networkAddr || undefined,
      data: data.data.toString('hex'),
      extension: data.extension || undefined,
    };
    const payload = Buffer.from(JSON.stringify(dldata));
    this.#dldata.sendMsg(payload, (err) => {
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
      this.#dldataResp.status() === Status.Connected &&
      this.#dldataResult.status() === Status.Connected
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
    if (queue.name() === this.#uldata.name()) {
      data.time = new Date(data.time);
      data.pub = new Date(data.pub);
      data.data = Buffer.from(data.data, 'hex');
      this.#mgrMsgHandler.onUlData(this, data, (err) => {
        if (err) {
          self.#uldata.nack(msg, (_err) => {});
        } else {
          self.#uldata.ack(msg, (_err) => {});
        }
      });
    } else if (queue.name() === this.#dldataResp.name()) {
      this.#mgrMsgHandler.onDlDataResp(this, data, (err) => {
        if (err) {
          self.#dldataResp.nack(msg, (_err) => {});
        } else {
          self.#dldataResp.ack(msg, (_err) => {});
        }
      });
    } else if (queue.name() === this.#dldataResult.name()) {
      this.#mgrMsgHandler.onDlDataResult(this, data, (err) => {
        if (err) {
          self.#dldataResult.nack(msg, (_err) => {});
        } else {
          self.#dldataResult.ack(msg, (_err) => {});
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
  #dldataResp;
  /** @type {AmqpQueue|MqttQueue} */
  #dldataResult;
  /** @type {Status} */
  #status;
  /** @type {AppMgrMsgHandlers} */
  #mgrMsgHandler;
}

const QUEUE_PREFIX = 'broker.application';

module.exports = {
  ApplicationMgr,
};
