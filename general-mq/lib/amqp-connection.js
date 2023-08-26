'use strict';

const { EventEmitter } = require('events');
const { URL } = require('url');

const amqplib = require('amqplib/callback_api');

const { DataTypes, Events, Status } = require('./constants');

const DEF_URI = 'amqp://localhost';
const DEF_CONN_TIMEOUT = 3000;
const DEF_RECONN = 1000;

/**
 * Connection error event.
 *
 * @event AmqpConnection#error
 * @type {Error}
 */

/**
 * Connection status event.
 *
 * @event AmqpConnection#status
 * @type {Status}
 */

/**
 * Manages an AMQP connection.
 *
 * @class AmqpConnection
 * @fires AmqpConnection#error
 * @fires AmqpConnection#status
 */
class AmqpConnection extends EventEmitter {
  /**
   * @constructor
   * @param {Object} [opts] The connection options.
   *   @param {string} [opts.uri='amqp://localhost'] Connection URI. Use
   *          `amqp|amqps://username:password@host:port/vhost` format.
   *   @param {number} [opts.connectTimeoutMillis=3000] Connection timeout in milliseconds.
   *   @param {number} [opts.reconnectMillis=1000] Time in milliseconds from disconnection to
   *          reconnection.
   *   @param {boolean} [opts.insecure=false] Allow insecure TLS connection.
   * @throws {Error} Wrong arguments.
   */
  constructor(opts) {
    super();

    if (opts === undefined) {
      opts = {};
    } else {
      if (!opts || typeof opts !== DataTypes.Object || Array.isArray(opts)) {
        throw Error('`opts` is not an object');
      }
      if (opts.uri !== undefined) {
        if (!opts.uri || typeof opts.uri !== DataTypes.String) {
          throw Error('`uri` is not a string');
        } else if (!URL.canParse(opts.uri)) {
          throw Error('`uri` is not a valid URI');
        }
        const u = new URL(opts.uri);
        if (u.protocol !== 'amqp:' && u.protocol !== 'amqps:') {
          throw Error('`uri` scheme only support `amqp` and `amqps`');
        }
      }
      if (
        opts.connectTimeoutMillis !== undefined &&
        (!Number.isInteger(opts.connectTimeoutMillis) || opts.connectTimeoutMillis < 0)
      ) {
        throw Error('`connectTimeoutMillis` is not a positive integer');
      } else if (
        opts.reconnectMillis !== undefined &&
        (!Number.isInteger(opts.reconnectMillis) || opts.reconnectMillis < 0)
      ) {
        throw Error('`reconnectMillis` is not a positive integer');
      } else if (opts.insecure !== undefined && typeof opts.insecure !== DataTypes.Boolean) {
        throw Error('`insecure` is not a boolean');
      }
    }

    this.#opts = {
      uri: opts.uri || DEF_URI,
      connectTimeoutMillis: opts.connectTimeoutMillis || DEF_CONN_TIMEOUT,
      reconnectMillis: opts.reconnectMillis || DEF_RECONN,
      insecure: opts.insecure || false,
    };
    this.#status = Status.Closed;
    this.#conn = null;
  }

  /**
   * To get the connection status.
   *
   * @returns {Status}
   */
  status() {
    return this.#status;
  }

  /**
   * To connect to the message broker. The `AmqpConnection` will report status with Status.
   */
  connect() {
    if (this.#status !== Status.Closed && this.#status !== Status.Closing) {
      return;
    }

    this.#status = Status.Connecting;
    this.emit(Events.Status, Status.Connecting);

    this.#innerConnect();
  }

  /**
   * To close the connection. You can use a callback function to get the result or listen events.
   *
   * @param {function} [callback]
   *   @param {?Error} callback.err
   */
  close(callback) {
    if (typeof callback !== DataTypes.Function) {
      callback = null;
    }

    if (!this.#conn) {
      if (callback) {
        return void process.nextTick(() => {
          callback(null);
        });
      }
      return;
    }

    this.#status = Status.Closing;
    this.emit(Events.Status, Status.Closing);
    const self = this;
    this.#conn.close((err) => {
      if (self.#conn) {
        self.#conn.removeAllListeners();
        self.#conn = null;
      }
      self.#status = Status.Closed;
      self.emit(Events.Status, Status.Closed);
      if (callback) {
        return void process.nextTick(() => {
          callback(err);
        });
      }
    });
  }

  /**
   * To get the raw AMQP connection instance for channel declaration.
   *
   * @private
   * @returns {?amqplib.Connection} The connection instance.
   */
  getRawConnection() {
    return this.#conn;
  }

  #innerConnect() {
    let self = this;
    const opts = {};
    if (this.#opts.insecure) {
      opts.rejectUnauthorized = false;
    }
    amqplib.connect(this.#opts.uri, opts, (err, conn) => {
      if (err) {
        return void setTimeout(() => {
          self.#innerConnect();
        }, self.#opts.reconnectMillis);
      }

      conn.on('close', self.#onClose.bind(self));
      conn.on('error', self.#onError.bind(self));
      self.#conn = conn;
      self.#status = Status.Connected;
      self.emit(Events.Status, Status.Connected);
    });
  }

  #onClose() {
    if (this.#conn) {
      this.#conn.removeAllListeners();
      this.#conn = null;
    }

    if (this.#status !== Status.Closing && this.#status !== Status.Closed) {
      this.#status = Status.Connecting;
      this.emit(Events.Status, Status.Connecting);
      this.#innerConnect();
    }
  }

  #onError(err) {
    if (this.#status !== Status.Closed) {
      this.emit(Events.Error, err);
    }
  }

  #opts;
  /** @type {Status} */
  #status;
  /** @type {amqplib.Connection} */
  #conn;
}

module.exports = {
  AmqpConnection,
};
