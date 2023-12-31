'use strict';

const { URL } = require('url');

const gmq = require('general-mq');
const { AmqpConnection } = require('general-mq/lib/amqp-connection');
const { AmqpQueue } = require('general-mq/lib/amqp-queue');
const { DataTypes } = require('general-mq/lib/constants');
const { MqttConnection } = require('general-mq/lib/mqtt-connection');
const { MqttQueue } = require('general-mq/lib/mqtt-queue');

const { Status } = require('./constants');

/**
 * Detail queue connection status.
 *
 * @typedef {Object} DataMqStatus
 * @property {Status} uldata
 * @property {Status} dldata
 * @property {Status} dldataResp
 * @property {Status} dldataResult
 * @property {Status} ctrl
 */

/**
 * The options of the application/network manager.
 *
 * @typedef {Object} Options
 * @property {string} [unitId] The associated unit ID of the application/network. Empty or undefined
 *           for public network.
 * @property {string} [unitCode] The associated unit code of the application/network. Empty or
 *           undefined for public network.
 * @property {string} id The associated application/network ID.
 * @property {string} name The associated application/network code.
 * @property {number} [prefetch=100] AMQP prefetch option.
 * @property {boolean} [persistent=false] AMQP persistent option.
 * @property {string} [sharedPrefix] MQTT shared queue prefix option.
 */

/**
 * @typedef {Object} CtrlAddDevice
 * @property {string} networkAddr
 */

/**
 * @typedef {Object} CtrlAddDeviceBulk
 * @property {string[]} networkAddrs
 */

/**
 * @typedef {Object} CtrlAddDeviceRange
 * @property {string} startAddr
 * @property {string} endAddr
 */

/**
 * @typedef {Object} CtrlDelDevice
 * @property {string} networkAddr
 */

/**
 * @typedef {Object} CtrlDelDeviceBulk
 * @property {string[]} networkAddrs
 */

/**
 * @typedef {Object} CtrlDelDeviceRange
 * @property {string} startAddr
 * @property {string} endAddr
 */

/**
 * @private
 * @typedef {Object} DataMqQueues
 * @property {AmqpQueue|MqttQueue} uldata
 * @property {AmqpQueue|MqttQueue} dldata
 * @property {?AmqpQueue|?MqttQueue} dldataResp (for application only)
 * @property {AmqpQueue|MqttQueue} dldataResult
 * @property {?AmqpQueue|?MqttQueue} ctrl (for network only)
 */

/**
 * The connection object with reference count for pool management.
 *
 * @class Connection
 */
class Connection {
  /**
   * @param {AmqpConnection|MqttConnection} conn
   * @throws {Error} Wrong host scheme.
   */
  constructor(conn) {
    if (!(conn instanceof AmqpConnection) && !(conn instanceof MqttConnection)) {
      throw Error('invalid `conn`');
    }

    this.conn = conn;
    this.count = 0;
  }

  /** @type {AmqpConnection|MqttConnection} */
  conn;
  /**
   * Reference count.
   *
   * @type {number}
   */
  count;
}

const DEF_PREFETCH = 100;
const DEF_PERSISTENT = false;

/**
 * Utility function to get the message queue connection instance. A new connection will be created
 * if the host does not exist.
 *
 * @private
 * @param {Map<string, Connection>} connPool
 * @param {URL} hostUri
 * @returns {Connection}
 * @throws {Error} Wrong host scheme.
 */
function getConnection(connPool, hostUri) {
  const uri = hostUri.toString();

  let conn = connPool.get(uri);
  if (conn) {
    return conn;
  }

  let engine;
  switch (hostUri.protocol) {
    case 'amqp:':
    case 'amqps:':
      engine = gmq.amqp;
      break;
    case 'mqtt:':
    case 'mqtts:':
      engine = gmq.mqtt;
      break;
    default:
      throw Error(`unsupport scheme ${hostUri.protocol}`);
  }
  const c = new engine.Connection({ uri });
  conn = new Connection(c);
  connPool.set(uri, conn);
  return conn;
}

/**
 * Utility function to remove connection from the pool if the reference count meet zero.
 *
 * @private
 * @param {Map<string, Connection>} connPool
 * @param {string} hostUri
 * @param {number} count
 * @param {function} callback
 *   @param {?Error} callback.err
 */
function removeConnection(connPool, hostUri, count, callback) {
  const conn = connPool.get(hostUri);
  if (!conn) {
    return void process.nextTick(() => {
      callback(null);
    });
  }

  conn.count -= count;
  if (conn.count <= 0) {
    connPool.delete(hostUri);
  }
  conn.conn.removeAllListeners();
  conn.conn.close((err) => {
    callback(err || null);
  });
}

/**
 * The utility function for creating application/network queue. The return object contains:
 * - `[prefix].[unit].[code].uldata`
 * - `[prefix].[unit].[code].dldata`
 * - `[prefix].[unit].[code].dldata-resp`
 * - `[prefix].[unit].[code].dldata-result`
 * - `[prefix].[unit].[code].ctrl`
 *
 * @private
 * @param {Connection} conn
 * @param {Options} opts
 * @param {string} prefix
 * @param {bool} isNetwork
 * @returns {DataMqQueues}
 * @throws {Error} Wrong parameters.
 */
function newDataQueues(conn, opts, prefix, isNetwork) {
  if (!(conn instanceof Connection)) {
    throw Error('`conn` is not a Connection');
  } else if (!opts || typeof opts !== DataTypes.Object || Array.isArray(opts)) {
    throw Error('`opts` is not an object');
  } else if (!prefix || typeof prefix !== DataTypes.String) {
    throw Error('`prefix` is not a string');
  } else if (typeof isNetwork !== DataTypes.Boolean) {
    throw Error('`isNetwork` is not a boolean');
  } else if (opts.unitId !== undefined && typeof opts.unitId !== DataTypes.String) {
    throw Error('`opts.unitId` is not a string');
  } else if (opts.unitCode !== undefined && typeof opts.unitCode !== DataTypes.String) {
    throw Error('`opts.unitCode` is not a string');
  } else if (!opts.id || typeof opts.id !== DataTypes.String) {
    throw Error('`opts.id` is not a non-empty string');
  } else if (!opts.name || typeof opts.name !== DataTypes.String) {
    throw Error('`opts.name` is not a non-empty string');
  } else if (
    opts.prefetch !== undefined &&
    (!Number.isInteger(opts.prefetch) || opts.prefetch < 0 || opts.prefetch > 65535)
  ) {
    throw Error('`opts.prefetch` is not an integer between 1 and 65535');
  } else if (opts.persistent !== undefined && typeof opts.persistent !== DataTypes.Boolean) {
    throw Error('`opts.persistent` is not a boolean');
  } else if (opts.sharedPrefix !== undefined && typeof opts.sharedPrefix !== DataTypes.String) {
    throw Error('`opts.sharedPrefix` is not a string');
  }

  if ((opts.unitId && !opts.unitCode) || (!opts.unitId && opts.unitCode)) {
    throw Error('`opts.unitId` and `opts.unitCode` must both empty or non-empty');
  }

  const engine = conn.conn instanceof MqttConnection ? gmq.mqtt : gmq.amqp;
  const qNamePrefix = `${prefix}.${opts.unitCode || '_'}.${opts.name}`;

  let qOpts = {
    name: `${qNamePrefix}.uldata`,
    isRecv: !isNetwork,
    reliable: true,
    broadcast: false,
    prefetch: opts.prefetch || DEF_PREFETCH,
    persistent: opts.persistent || DEF_PERSISTENT,
    sharedPrefix: opts.sharedPrefix,
  };
  const uldata = new engine.Queue(qOpts, conn.conn);

  qOpts.name = `${qNamePrefix}.dldata`;
  qOpts.isRecv = isNetwork;
  const dldata = new engine.Queue(qOpts, conn.conn);

  qOpts.name = `${qNamePrefix}.dldata-resp`;
  qOpts.isRecv = !isNetwork;
  const dldataResp = isNetwork ? null : new engine.Queue(qOpts, conn.conn);

  qOpts.name = `${qNamePrefix}.dldata-result`;
  qOpts.isRecv = !isNetwork;
  const dldataResult = new engine.Queue(qOpts, conn.conn);

  qOpts.name = `${qNamePrefix}.ctrl`;
  qOpts.isRecv = true;
  const ctrl = isNetwork ? new engine.Queue(qOpts, conn.conn) : null;

  return {
    uldata,
    dldata,
    dldataResp,
    dldataResult,
    ctrl,
  };
}

module.exports = {
  Connection,
  getConnection,
  removeConnection,
  newDataQueues,
};
