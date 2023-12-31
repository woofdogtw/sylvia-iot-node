'use strict';

const { EventEmitter } = require('events');
const { URL } = require('url');

const mqtt = require('mqtt');
const randomstring = require('randomstring');

const { DataTypes, Events, Status, QueuePattern } = require('./constants');

const DEF_URI = 'mqtt://localhost';
const DEF_CONN_TIMEOUT = 3000;
const DEF_RECONN = 1000;

/**
 * Connection error event.
 *
 * @event MqttConnection#error
 * @type {Error}
 */

/**
 * Connection status event.
 *
 * @event MqttConnection#status
 * @type {Status}
 */

/**
 * Packet handler object.
 *
 * @typedef {Object} PacketHandler
 * @property {string} topic The relative topic of the queue (with shared prefix or not).
 * @property {number} qos 0 for best-effort and 1 for reliable.
 * @property {function} handler
 *   @param {Buffer} handler.payload
 */

/**
 * Manages an MQTT connection.
 *
 * @class MqttConnection
 * @fires MqttConnection#error
 * @fires MqttConnection#status
 */
class MqttConnection extends EventEmitter {
  /**
   * @constructor
   * @param {Object} [opts] The connection options.
   *   @param {string} [opts.uri='mqtt://localhost'] Connection URI. Use
   *          `mqtt|mqtts://username:password@host:port/vhost` format.
   *   @param {number} [opts.connectTimeoutMillis=3000] Connection timeout in milliseconds.
   *   @param {number} [opts.reconnectMillis=1000] Time in milliseconds from disconnection to
   *          reconnection.
   *   @param {boolean} [opts.insecure=false] Allow insecure TLS connection.
   *   @param {string} [opts.clientId] Client identifier. Default uses a random client identifier.
   *   @param {boolean} [opts.cleanSession=true] Clean session flag. This is NOT stable.
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
        if (u.protocol !== 'mqtt:' && u.protocol !== 'mqtts:') {
          throw Error('`uri` scheme only support `mqtt` and `mqtts`');
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
      } else if (
        opts.clientId !== undefined &&
        (typeof opts.clientId !== DataTypes.String ||
          opts.clientId.length <= 0 ||
          opts.clientId.length > 23)
      ) {
        throw Error('`clientId` is not a string with length 1~23');
      } else if (
        opts.cleanSession !== undefined &&
        typeof opts.cleanSession !== DataTypes.Boolean
      ) {
        throw Error('`cleanSession` is not a boolean');
      }
    }

    this.#opts = {
      uri: opts.uri || DEF_URI,
      connectTimeoutMillis: opts.connectTimeoutMillis || DEF_CONN_TIMEOUT,
      reconnectMillis: opts.reconnectMillis || DEF_RECONN,
      insecure: opts.insecure || false,
      clientId: opts.clientId || `general-mq-${randomstring.generate(12)}`,
      cleanSession: !(opts.cleanSession === false),
    };
    this.#status = Status.Closed;
    this.#conn = null;
    this.#packetHandlers = new Map();
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
    this.#conn.end((err) => {
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
   * To get the raw MQTT connection instance for topic subscription.
   *
   * @private
   * @returns {?mqtt.MqttClient} The connection instance.
   */
  getRawConnection() {
    return this.#conn;
  }

  /**
   * To add a packet handler for `MqttQueue`.
   *
   * @private
   * @param {string} name The queue name.
   * @param {string} topic The relative topic.
   * @param {boolean} reliable The queue is reliable.
   * @param {function} handler The packet handler.
   *   @param {Buffer} handler.payload The packet payload.
   */
  addPacketHandler(name, topic, reliable, handler) {
    if (!QueuePattern.test(name)) {
      throw Error('`name` is not a valid queue name');
    } else if (!topic || typeof topic !== DataTypes.String || !topic.endsWith(name)) {
      throw Error('`topic` is not a valid topic');
    } else if (typeof reliable !== DataTypes.Boolean) {
      throw Error('`reliable` is not a boolean');
    } else if (typeof handler !== DataTypes.Function) {
      throw Error('`handler` is not a function');
    }

    this.#packetHandlers.set(name, {
      topic,
      qos: reliable ? 1 : 0,
      handler,
    });
  }

  /**
   * To remove a packet handler.
   *
   * @private
   * @param {string} name The queue name.
   */
  removePacketHandler(name) {
    if (!QueuePattern.test(name)) {
      throw Error('`name` is not a valid queue name');
    }

    this.#packetHandlers.delete(name);
  }

  #innerConnect() {
    const urlInfo = new URL(this.#opts.uri);
    const opts = {
      reconnectPeriod: this.#opts.reconnectMillis,
      connectTimeout: this.#opts.connectTimeoutMillis,
      clean: this.#opts.cleanSession,
      username: urlInfo.username,
      password: urlInfo.password,
    };
    if (this.#opts.insecure) {
      opts.rejectUnauthorized = false;
    }

    this.#conn = mqtt.connect(this.#opts.uri, opts);
    this.#conn.on('close', this.#onClose.bind(this));
    this.#conn.on('connect', this.#onConnect.bind(this));
    this.#conn.on('error', this.#onError.bind(this));
    this.#conn.on('message', this.#onMessage.bind(this));
    this.#conn.on('offline', this.#onClose.bind(this));
    this.#conn.on('reconnect', this.#onReconnect.bind(this));
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

  #onConnect() {
    if (this.#conn) {
      this.#status = Status.Connected;
      this.emit(Events.Status, Status.Connected);
    }
  }

  #onError(err) {
    if (this.#status !== Status.Closed) {
      this.emit(Events.Error, err);
    }
  }

  #onMessage(topic, message, _packet) {
    const handler = this.#packetHandlers.get(topic);
    if (handler) {
      handler.handler(message);
    }
  }

  #onReconnect() {
    // Rely on library's reconnect instead of calling #innerConnect().
    if (this.#status !== Status.Closing && this.#status !== Status.Closed) {
      this.#status = Status.Connecting;
      this.emit(Events.Status, Status.Connecting);
    }
  }

  #opts;
  /** @type {Status} */
  #status;
  /** @type {mqtt.MqttClient} */
  #conn;
  /** @type {Map<string, PacketHandler>} */
  #packetHandlers;
}

module.exports = {
  MqttConnection,
};
