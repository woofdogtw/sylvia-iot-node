'use strict';

const { EventEmitter } = require('events');

const amqplib = require('amqplib/callback_api');
const async = require('async');

const { AmqpConnection } = require('./amqp-connection');
const { DataTypes, Errors, Events, QueuePattern, Status } = require('./constants');

const DEF_RECONN = 1000;

const amqplibConsts = {
  Fanout: 'fanout',
};

/**
 * Queue error event.
 *
 * @event AmqpQueue#error
 * @type {Error}
 */

/**
 * Queue status event.
 *
 * @event AmqpQueue#status
 * @type {Status}
 */

/**
 * The message that contains payload and meta data for acknowledgement.
 *
 * @typedef {Object} AmqpMessage
 * @property {Buffer} payload The message payload.
 * @property {amqplib.Message} meta The meta data that is used for acknowledgement. You should not
 *           access this property.
 */

/**
 * Message handler.
 *
 * @callback AmqpQueueMsgHandler
 * @param {AmqpQueue} queue The relative queue that receives the incoming message.
 * @param {AmqpMessage} msg The incoming message.
 */

/**
 * Manages an AMQP queue.
 *
 * @class AmqpQueue
 * @fires AmqpQueue#error
 * @fires AmqpQueue#status
 */
class AmqpQueue extends EventEmitter {
  /**
   * @constructor
   * @param {Object} opts The queue options.
   *   @param {string} opts.name The queue name that is used to map a AMQP queue (unicast) or an
   *          exchange (broadcast). The pattern is `^[a-z0-9_-]+([\.]{1}[a-z0-9_-]+)*$`.
   *   @param {boolean} opts.isRecv `true` for the receiver and `false` for the sender.
   *   @param {boolean} opts.reliable Reliable by selecting the confirm channel (for publish).
   *   @param {boolean} opts.broadcast `true` for broadcast and `false` for unicast.
   *   @param {number} [opts.reconnectMillis=1000] Time in milliseconds from disconnection to
   *          reconnection.
   *   @param {number} [opts.prefetch] REQUIRED when `isRecv=true`. The QoS of the receiver queue.
   *          This value MUST be a positive value between 1 to 65535.
   *   @param {boolean} [opts.persistent=false] Use persistent delivery mode.
   * @param {AmqpConnection} conn The AMQP connection.
   * @throws {Error} Wrong arguments.
   */
  constructor(opts, conn) {
    super();

    if (!opts || typeof opts !== DataTypes.Object || Array.isArray(opts)) {
      throw Error('`opts` is not an object');
    } else if (!(conn instanceof AmqpConnection)) {
      throw Error('`conn` is not a `AmqpConnection` object');
    } else if (!QueuePattern.test(opts.name)) {
      throw Error('`name` is not match pattern `^[a-z0-9_-]+([\\.]{1}[a-z0-9_-]+)*$`');
    } else if (typeof opts.isRecv !== DataTypes.Boolean) {
      throw Error('`isRecv` is not boolean');
    } else if (typeof opts.reliable !== DataTypes.Boolean) {
      throw Error('`reliable` is not boolean');
    } else if (typeof opts.broadcast !== DataTypes.Boolean) {
      throw Error('`broadcast` is not boolean');
    } else if (
      opts.isRecv &&
      (!Number.isInteger(opts.prefetch) || opts.prefetch <= 0 || opts.prefetch > 65535)
    ) {
      throw Error('`prefetch` must be a positive integer between 1 to 65535');
    }
    if (
      opts.reconnectMillis !== undefined &&
      (!Number.isInteger(opts.reconnectMillis) || opts.reconnectMillis < 0)
    ) {
      throw Error('`reconnectMillis` must be a positive integer');
    }
    if (opts.persistent !== undefined && typeof opts.persistent !== DataTypes.Boolean) {
      throw Error('`persistent` is not boolean');
    }

    this.#opts = {
      name: opts.name,
      isRecv: opts.isRecv,
      reliable: opts.reliable,
      broadcast: opts.broadcast,
      reconnectMillis: opts.reconnectMillis || DEF_RECONN,
      prefetch: opts.prefetch,
      persistent: opts.persistent || false,
    };
    this.#status = Status.Closed;
    this.#conn = conn;
    this.#connProcessing = false;
    this.#channel = null;
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
   * @param {?AmqpQueueMsgHandler} handler
   * @throws {Error} Wrong arguments.
   */
  setMsgHandler(handler) {
    if (typeof handler !== DataTypes.Function) {
      throw Error('the handler is not a function');
    }

    this.#msgHandler = handler;
  }

  /**
   * To connect to the message queue. The `AmqpQueue` will report status with Status.
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

    if (this.#status === Status.Closing || this.#status === Status.Closed) {
      if (callback) {
        return void process.nextTick(() => {
          callback(null);
        });
      }
      return;
    } else if (!this.#channel) {
      this.#status = Status.Closed;
      this.emit(Events.Status, Status.Closed);
      if (callback) {
        return void process.nextTick(() => {
          callback(null);
        });
      }
    }

    this.#status = Status.Closing;
    this.emit(Events.Status, Status.Closing);
    const self = this;
    this.#channel.close((err) => {
      if (self.#channel) {
        self.#channel.removeAllListeners();
        self.#channel = null;
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

    const exchange = this.#opts.broadcast ? this.#opts.name : '';
    const routingKey = this.#opts.broadcast ? '' : this.#opts.name;
    let opts = {
      persistent: this.#opts.persistent,
    };
    if (this.#opts.reliable) {
      opts = {
        mandatory: true,
      };
      this.#channel.publish(exchange, routingKey, payload, opts, (err, ok) => {
        if (err) {
          return void callback(err);
        }
        callback(null);
      });
    } else {
      this.#channel.publish(exchange, routingKey, payload, opts);
      // Use `setTimeout` instead of `nextTick` because nextTick may causes too much events to hang
      // AMQP packets transmission.
      setTimeout(() => {
        callback(null);
      }, 1);
    }
  }

  /**
   * Use this if the message is processed successfully.
   *
   * @param {AmqpMessage} msg
   * @param {function} callback
   *   @param {?Error} callback.err
   */
  ack(msg, callback) {
    if (!msg || typeof msg !== DataTypes.Object || Array.isArray(msg)) {
      throw Error('`msg` is not an object');
    } else if (typeof callback !== DataTypes.Function) {
      throw Error('`callback` is not a function');
    }

    const channel = this.#channel;
    if (channel) {
      channel.ack(msg.meta);
    }
    process.nextTick(() => {
      callback(null);
    });
  }

  /**
   * To requeue the message and the broker will send the message in the future.
   *
   * @param {AmqpMessage} msg
   * @param {function} callback
   *   @param {?Error} callback.err
   */
  nack(msg, callback) {
    if (!msg || typeof msg !== DataTypes.Object || Array.isArray(msg)) {
      throw Error('`msg` is not an object');
    } else if (typeof callback !== DataTypes.Function) {
      throw Error('`callback` is not a function');
    }

    const channel = this.#channel;
    if (channel) {
      channel.nack(msg.meta);
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

    let channel;
    async.waterfall(
      [
        // Create a channel.
        function (cb) {
          const fn = self.#opts.reliable
            ? rawConn.createConfirmChannel.bind(rawConn)
            : rawConn.createChannel.bind(rawConn);
          fn((err, ch) => {
            if (err) {
              return void cb(err);
            }
            channel = ch;
            cb(null);
          });
        },
        // Declare resources for unicast or broadcast.
        function (cb) {
          if (self.#opts.broadcast) {
            self.#createBroadcast(channel, (err, qname) => {
              cb(err, qname);
            });
          } else {
            self.#createUnicast(channel, (err) => {
              if (err) {
                return void cb(err);
              }
              cb(null, self.#opts.name);
            });
          }
        },
        // Set prefetch and consume the incoming messages.
        function (qname, cb) {
          if (!self.#opts.isRecv) {
            return void cb(null);
          }

          channel.prefetch(self.#opts.prefetch);
          channel.consume(qname, self.#innerOnMessage.bind(self), {}, (err) => {
            cb(err);
          });
        },
      ],
      (err) => {
        self.#connProcessing = false;
        if (err) {
          self.emit(Events.Error, err);
          return void setTimeout(() => {
            self.#innerConnect();
          }, self.#opts.reconnectMillis);
        }

        channel.on('close', self.#onClose.bind(self));
        channel.on('drain', self.#onDrain.bind(self));
        channel.on('error', self.#onError.bind(self));
        channel.on('return', self.#onReturn.bind(self));
        self.#channel = channel;
        self.#status = Status.Connected;
        self.emit(Events.Status, Status.Connected);
      }
    );
  }

  /**
   * Message handler. `this` is the `AmqpQueue` instance.
   *
   * @param {amqplib.Message} msg The raw message from the amqplib library.
   */
  #innerOnMessage(msg) {
    const handler = this.#msgHandler;
    if (handler) {
      handler(this, {
        payload: msg.content,
        meta: msg,
      });
    }
  }

  /**
   * To create resouces for the broadcast queue.
   *
   * @param {amqplib.Channel} channel
   * @param {function} callback
   *   @param {?Error} callback.err
   *   @param {string} callback.qname The name of the temporary queue.
   */
  #createBroadcast(channel, callback) {
    const self = this;

    async.waterfall(
      [
        // Declare the fanout exchange.
        function (cb) {
          const opts = { durable: false };
          channel.assertExchange(self.#opts.name, amqplibConsts.Fanout, opts, (err) => {
            cb(err);
          });
        },
        // Declare a temporary queue and bind the queue name to the exchange.
        function (cb) {
          if (!self.#opts.isRecv) {
            return void cb(null, '');
          }

          channel.assertQueue('', { exclusive: true }, (err, q) => {
            if (err) {
              return void cb(err);
            }
            channel.bindQueue(q.queue, self.#opts.name, '', {}, (err) => {
              cb(err, q.name);
            });
          });
        },
      ],
      (err, qname) => {
        callback(err, qname);
      }
    );
  }

  /**
   * To create resouces for the unicast queue.
   *
   * @param {amqplib.Channel} channel
   * @param {function} callback
   *   @param {?Error} callback.err
   */
  #createUnicast(channel, callback) {
    channel.assertQueue(this.#opts.name, { durable: true }, (err) => {
      callback(err);
    });
  }

  /**
   * To handle `AmqpConnection` status change events.
   *
   * @param {Status} status The latest status of the relative `AmqpConnection`.
   */
  #onConnStatusChanged(status) {
    switch (status) {
      case Status.Closed:
      case Status.Closing:
      case Status.Connecting:
      case Status.Disconnected:
        this.#onClose();
        return;
      case Status.Connected:
        return void this.#innerConnect();
      default:
        return;
    }
  }

  #onClose() {
    if (this.#channel) {
      this.#channel.removeAllListeners();
      this.#channel = null;
    }

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
  }

  #onDrain() {}

  #onError(_err) {}

  #onReturn(_msg) {}

  #opts;
  /** @type {Status} */
  #status;
  /** @type {AmqpConnection} */
  #conn;
  /**
   * Processing `#innerConnect`.
   *
   * @type {boolean}
   */
  #connProcessing;
  /** @type {amqplib.Channel} */
  #channel;
  /** @type {AmqpQueueMsgHandler} */
  #msgHandler;
}

module.exports = {
  AmqpQueue,
};
