'use strict';

const { EventEmitter } = require('events');

const { MqttConnection } = require('./mqtt-connection');
const { DataTypes, Errors, Events, QueuePattern, Status } = require('./constants');

const DEF_RECONN = 1000;

/**
 * Queue error event.
 *
 * @event MqttQueue#error
 * @type {Error}
 */

/**
 * Queue status event.
 *
 * @event MqttQueue#status
 * @type {Status}
 */

/**
 * The message that contains payload and meta data for acknowledgement.
 *
 * @typedef {Object} MqttMessage
 * @property {Buffer} payload The message payload.
 */

/**
 * Message handler.
 *
 * @callback MqttQueueMsgHandler
 * @param {MqttQueue} queue The relative queue that receives the incoming message.
 * @param {MqttMessage} msg The incoming message.
 */

/**
 * Manages an MQTT queue.
 *
 * @class MqttQueue
 * @fires MqttQueue#error
 * @fires MqttQueue#status
 */
class MqttQueue extends EventEmitter {
  /**
   * @constructor
   * @param {Object} opts The queue options.
   *   @param {string} opts.name The queue name that is used to map a MQTT queue (unicast) or an
   *          exchange (broadcast). The pattern is `^[a-z0-9_-]+([\.]{1}[a-z0-9_-]+)*$`.
   *   @param {boolean} opts.isRecv `true` for the receiver and `false` for the sender.
   *   @param {boolean} opts.reliable Reliable by selecting the confirm channel (for publish).
   *   @param {boolean} opts.broadcast `true` for broadcast and `false` for unicast.
   *   @param {number} [opts.reconnectMillis=1000] Time in milliseconds from disconnection to
   *          reconnection.
   *   @param {string} [opts.sharedPrefix] OPTIONAL when `isRecv=true`. This is used for unicast.
   * @param {MqttConnection} conn The MQTT connection.
   * @throws {Error} Wrong arguments.
   */
  constructor(opts, conn) {
    super();

    if (!opts || typeof opts !== DataTypes.Object || Array.isArray(opts)) {
      throw Error('`opts` is not an object');
    } else if (!(conn instanceof MqttConnection)) {
      throw Error('`conn` is not a `MqttConnection` object');
    } else if (!QueuePattern.test(opts.name)) {
      throw Error('`name` is not match pattern `^[a-z0-9_-]+([\\.]{1}[a-z0-9_-]+)*$`');
    } else if (typeof opts.isRecv !== DataTypes.Boolean) {
      throw Error('`isRecv` is not boolean');
    } else if (typeof opts.reliable !== DataTypes.Boolean) {
      throw Error('`reliable` is not boolean');
    } else if (typeof opts.broadcast !== DataTypes.Boolean) {
      throw Error('`broadcast` is not boolean');
    } else if (
      opts.sharedPrefix !== undefined &&
      (!opts.sharedPrefix || typeof opts.sharedPrefix !== DataTypes.String)
    ) {
      throw Error('`sharedPrefix` must be a string');
    }
    if (
      opts.reconnectMillis !== undefined &&
      (!Number.isInteger(opts.reconnectMillis) || opts.reconnectMillis < 0)
    ) {
      throw Error('`reconnectMillis` must be a positive integer');
    }

    this.#opts = {
      name: opts.name,
      isRecv: opts.isRecv,
      reliable: opts.reliable,
      broadcast: opts.broadcast,
      reconnectMillis: opts.reconnectMillis || DEF_RECONN,
      sharedPrefix: opts.sharedPrefix,
    };
    this.#status = Status.Closed;
    this.#conn = conn;
    this.#connProcessing = false;
    this.#msgHandler = null;

    this.#conn.on(Events.Status, this.#onConnStatusChanged.bind(this));
  }

  /**
   * To get the queue name.
   *
   * @returns {string}
   */
  name() {
    return this.#opts.name;
  }

  /**
   * Is the queue a receiver.
   *
   * @returns {boolean}
   */
  isRecv() {
    return this.#opts.isRecv;
  }

  /**
   * To get the queue status.
   *
   * @returns {Status}
   */
  status() {
    return this.#status;
  }

  /**
   * Set the message handler.
   *
   * @param {?MqttQueueMsgHandler} handler
   * @throws {Error} Wrong arguments.
   */
  setMsgHandler(handler) {
    if (typeof handler !== DataTypes.Function) {
      throw Error('the handler is not a function');
    }

    this.#msgHandler = handler;
  }

  /**
   * To connect to the message queue. The `MqttQueue` will report status with Status.
   *
   * @throws {Error} Wrong usage.
   */
  connect() {
    if (this.#opts.isRecv && !this.#msgHandler) {
      throw Error(Errors.NoMsgHandler);
    }

    if (this.#status !== Status.Closed && this.#status !== Status.Closing) {
      return;
    }

    if (this.#opts.isRecv) {
      this.#conn.addPacketHandler(
        this.#opts.name,
        this.#topic(),
        this.#opts.reliable,
        this.#innerOnMessage.bind(this)
      );
    }
    this.#status = Status.Connecting;
    this.emit(Events.Status, Status.Connecting);

    this.#innerConnect();
  }

  /**
   * To close the queue. You can use a callback function to get the result or listen events.
   *
   * @param {function} [callback]
   *   @param {?Error} callback.err
   */
  close(callback) {
    if (typeof callback !== DataTypes.Function) {
      callback = null;
    }

    if (
      this.#status === Status.Closing ||
      this.#status === Status.Closed ||
      !this.#conn.getRawConnection()
    ) {
      if (callback) {
        return void process.nextTick(() => {
          callback(null);
        });
      }
      return;
    }

    const rawConn = this.#conn.getRawConnection();
    this.#status = Status.Closing;
    this.emit(Events.Status, Status.Closing);
    const self = this;
    rawConn.unsubscribe(this.#topic(), (err) => {
      if (self.#opts.isRecv) {
        self.#conn.removePacketHandler(self.#opts.name);
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
   * To send a message (for senders only).
   *
   * @param {Buffer} payload The raw data to be sent.
   * @param {function} callback
   *   @param {?Error} callback.err
   * @throws {Error} Wrong arguments.
   */
  sendMsg(payload, callback) {
    if (!(payload instanceof Buffer)) {
      throw Error('`payload` is not a Buffer');
    } else if (typeof callback !== DataTypes.Function) {
      throw Error('`callback` is not a function');
    } else if (this.#status !== Status.Connected) {
      return void process.nextTick(() => {
        callback(Error(Errors.NotConnected));
      });
    }
    if (this.#opts.isRecv) {
      return void process.nextTick(() => {
        callback(Error(Errors.QueueIsReceiver));
      });
    }

    const rawConn = this.#conn.getRawConnection();
    const opts = {
      qos: this.#opts.reliable ? 1 : 0,
    };
    rawConn.publish(this.#topic(), payload, opts, (err) => {
      callback(err);
    });
  }

  /**
   * Use this if the message is processed successfully.
   *
   * @param {MqttMessage} msg
   * @param {function} callback
   *   @param {?Error} callback.err
   */
  ack(msg, callback) {
    if (!msg || typeof msg !== DataTypes.Object || Array.isArray(msg)) {
      throw Error('`msg` is not an object');
    } else if (typeof callback !== DataTypes.Function) {
      throw Error('`callback` is not a function');
    }

    process.nextTick(() => {
      callback(null);
    });
  }

  /**
   * To requeue the message and the broker will send the message in the future.
   *
   * @param {MqttMessage} msg
   * @param {function} callback
   *   @param {?Error} callback.err
   */
  nack(msg, callback) {
    if (!msg || typeof msg !== DataTypes.Object || Array.isArray(msg)) {
      throw Error('`msg` is not an object');
    } else if (typeof callback !== DataTypes.Function) {
      throw Error('`callback` is not a function');
    }

    process.nextTick(() => {
      callback(null);
    });
  }

  #innerConnect() {
    if (this.#status !== Status.Connecting || this.#connProcessing) {
      return;
    }
    this.#connProcessing = true;

    const self = this;

    if (this.#conn.status() !== Status.Connected) {
      this.#connProcessing = false;
      return void setTimeout(() => {
        self.#innerConnect();
      }, this.#opts.reconnectMillis);
    }
    const rawConn = this.#conn.getRawConnection();
    if (!rawConn) {
      this.#connProcessing = false;
      return void setTimeout(() => {
        self.#innerConnect();
      }, this.#opts.reconnectMillis);
    }

    if (!this.#opts.isRecv) {
      this.#connProcessing = false;
      this.#status = Status.Connected;
      this.emit(Events.Status, Status.Connected);
      return;
    }

    const opts = {
      qos: this.#opts.reliable ? 1 : 0,
    };
    rawConn.subscribe(this.#topic(), opts, (err) => {
      self.#connProcessing = false;
      if (err) {
        return void setTimeout(() => {
          self.#innerConnect();
        }, self.#opts.reconnectMillis);
      }

      self.#status = Status.Connected;
      self.emit(Events.Status, Status.Connected);
    });
  }

  /**
   * Message handler. `this` is the `MqttQueue` instance.
   *
   * @param {Buffer} payload The message payload.
   */
  #innerOnMessage(payload) {
    const handler = this.#msgHandler;
    if (handler) {
      handler(this, {
        payload,
      });
    }
  }

  /**
   * To get the associated topic.
   *
   * @returns {string}
   */
  #topic() {
    if (this.#opts.isRecv && !this.#opts.broadcast) {
      return `${this.#opts.sharedPrefix}${this.#opts.name}`;
    }
    return this.#opts.name;
  }

  /**
   * To handle `MqttConnection` status change events.
   *
   * @param {Status} status The latest status of the relative `MqttConnection`.
   */
  #onConnStatusChanged(status) {
    switch (status) {
      case Status.Closed:
      case Status.Closing:
      case Status.Connecting:
      case Status.Disconnected:
        if (
          this.#status === Status.Closing ||
          this.#status === Status.Closed ||
          this.#status === Status.Connecting
        ) {
          return;
        }
        this.#status = Status.Connecting;
        this.emit(Events.Status, Status.Connecting);
        const self = this;
        setTimeout(() => {
          self.#innerConnect();
        }, this.#opts.reconnectMillis);
        return;
      case Status.Connected:
        return void this.#innerConnect();
      default:
        return;
    }
  }

  #opts;
  /** @type {Status} */
  #status;
  /** @type {MqttConnection} */
  #conn;
  /**
   * Processing `#innerConnect`.
   *
   * @type {boolean}
   */
  #connProcessing;
  /** @type {MqttQueueMsgHandler} */
  #msgHandler;
}

module.exports = {
  MqttQueue,
};
