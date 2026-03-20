'use strict';

const { EventEmitter } = require('events');

const amqplib = require('amqplib');

const { AmqpConnection } = require('./amqp-connection');
const { DataTypes, Errors, Events, QueuePattern, Status } = require('./constants');
const { SdkError } = require('./lib');

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
 * Connection status changed handler.
 *
 * @callback OnConnStatusChanged
 * @param {Status} status The latest status of the connection.
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

    this.#onConnStatusChangedBound = this.#onConnStatusChanged.bind(this);
    this.#conn.on(Events.Status, this.#onConnStatusChangedBound);
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
   * To close the queue. You can use `await` to get the result or listen events.
   *
   * @async
   * @returns {Promise<void>}
   * @throws {SdkError}
   */
  async close() {
    this.#conn.removeListener(Events.Status, this.#onConnStatusChangedBound);
    if (this.#status === Status.Closing || this.#status === Status.Closed) {
      return;
    } else if (!this.#channel) {
      this.#status = Status.Closed;
      this.emit(Events.Status, Status.Closed);
      return;
    }

    this.#status = Status.Closing;
    this.emit(Events.Status, Status.Closing);

    let err;
    await this.#channel.close().catch((e) => (err = e));
    if (this.#channel) {
      this.#channel.removeAllListeners();
      this.#channel = null;
    }
    this.#status = Status.Closed;
    this.emit(Events.Status, Status.Closed);
    if (err) {
      throw new SdkError(err.message);
    }
  }

  /**
   * To send a message (for senders only).
   *
   * @async
   * @param {Buffer} payload The raw data to be sent.
   * @returns {Promise<void>}
   * @throws {Error} Wrong arguments.
   * @throws {SdkError}
   */
  async sendMsg(payload) {
    if (!(payload instanceof Buffer)) {
      throw Error('`payload` is not a Buffer');
    } else if (this.#status !== Status.Connected) {
      throw new SdkError(Errors.NotConnected);
    } else if (this.#opts.isRecv) {
      throw new SdkError(Errors.QueueIsReceiver);
    }

    const exchange = this.#opts.broadcast ? this.#opts.name : '';
    const routingKey = this.#opts.broadcast ? '' : this.#opts.name;
    if (this.#opts.reliable) {
      await new Promise((resolve, reject) => {
        const opts = { mandatory: true, persistent: this.#opts.persistent };
        this.#channel.publish(exchange, routingKey, payload, opts, (err) => {
          if (err) {
            return reject(new SdkError(err.message));
          }
          resolve();
        });
      });
    } else {
      this.#channel.publish(exchange, routingKey, payload, { persistent: this.#opts.persistent });
      // Use `setTimeout` instead of `nextTick` because nextTick may causes too much events to hang
      // AMQP packets transmission.
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
  }

  /**
   * Use this if the message is processed successfully.
   *
   * @async
   * @param {AmqpMessage} msg
   * @returns {Promise<void>}
   * @throws {Error} Wrong usage.
   * @throws {SdkError}
   */
  async ack(msg) {
    if (!msg || typeof msg !== DataTypes.Object || Array.isArray(msg)) {
      throw Error('`msg` is not an object');
    }

    if (this.#channel) {
      this.#channel.ack(msg.meta);
      await new Promise((resolve) => process.nextTick(resolve));
    }
  }

  /**
   * To requeue the message and the broker will send the message in the future.
   *
   * @async
   * @param {AmqpMessage} msg
   * @returns {Promise<void>}
   * @throws {Error} Wrong usage.
   * @throws {SdkError}
   */
  async nack(msg) {
    if (!msg || typeof msg !== DataTypes.Object || Array.isArray(msg)) {
      throw Error('`msg` is not an object');
    }

    if (this.#channel) {
      this.#channel.nack(msg.meta);
      await new Promise((resolve) => process.nextTick(resolve));
    }
  }

  async #innerConnect() {
    if (this.#status !== Status.Connecting || this.#connProcessing) {
      return;
    }
    this.#connProcessing = true;

    if (this.#conn.status() !== Status.Connected) {
      this.#connProcessing = false;
      return void setTimeout(() => this.#innerConnect(), this.#opts.reconnectMillis);
    }
    const rawConn = this.#conn.getRawConnection();
    if (!rawConn) {
      this.#connProcessing = false;
      return void setTimeout(() => this.#innerConnect(), this.#opts.reconnectMillis);
    }

    try {
      // Create a channel.
      const channel = this.#opts.reliable
        ? await rawConn.createConfirmChannel()
        : await rawConn.createChannel();

      if (this.#status !== Status.Connecting) {
        channel.close().catch(() => {});
        this.#connProcessing = false;
        return;
      }

      // Declare resources for unicast or broadcast.
      let qname;
      if (this.#opts.broadcast) {
        qname = await this.#createBroadcast(channel);
      } else {
        await this.#createUnicast(channel);
        qname = this.#opts.name;
      }

      if (this.#status !== Status.Connecting) {
        channel.close().catch(() => {});
        this.#connProcessing = false;
        return;
      }

      this.#connProcessing = false;

      channel.on('close', this.#onClose.bind(this));
      channel.on('drain', this.#onDrain.bind(this));
      channel.on('error', this.#onError.bind(this));
      channel.on('return', this.#onReturn.bind(this));
      this.#channel = channel;

      // Set prefetch and consume the incoming messages.
      if (this.#opts.isRecv) {
        await channel.prefetch(this.#opts.prefetch);
        await channel.consume(qname, this.#innerOnMessage.bind(this), {});
      }

      if (this.#status !== Status.Connecting) {
        this.#channel.removeAllListeners();
        this.#channel.close().catch(() => {});
        this.#channel = null;
        this.#connProcessing = false;
        return;
      }

      this.#status = Status.Connected;
      this.emit(Events.Status, Status.Connected);
    } catch (err) {
      if (this.#channel) {
        this.#channel.removeAllListeners();
        this.#channel = null;
      }
      this.#connProcessing = false;
      this.emit(Events.Error, err);
      return void setTimeout(() => this.#innerConnect(), this.#opts.reconnectMillis);
    }
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
   * To create resources for the broadcast queue.
   *
   * @async
   * @param {amqplib.Channel} channel
   * @returns {Promise<string>} The name of the temporary queue.
   * @throws {SdkError}
   */
  async #createBroadcast(channel) {
    try {
      // Declare the fanout exchange.
      await channel.assertExchange(this.#opts.name, amqplibConsts.Fanout, { durable: false });

      // Declare a temporary queue and bind the queue name to the exchange.
      if (!this.#opts.isRecv) {
        return '';
      }

      const q = await channel.assertQueue('', { exclusive: true });
      await channel.bindQueue(q.queue, this.#opts.name, '', {});
      return q.queue;
    } catch (err) {
      throw new SdkError(err.message);
    }
  }

  /**
   * To create resouces for the unicast queue.
   *
   * @async
   * @param {amqplib.Channel} channel
   * @returns {Promise<void>}
   * @throws {SdkError}
   */
  async #createUnicast(channel) {
    await channel.assertQueue(this.#opts.name, { durable: true }).catch((err) => {
      throw new SdkError(err.message);
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
      this.#status === Status.Connecting ||
      this.#status === Status.Disconnected
    ) {
      return;
    }
    this.#status = Status.Disconnected;
    this.emit(Events.Status, Status.Disconnected);
    setTimeout(() => {
      if (this.#status !== Status.Closing && this.#status !== Status.Closed) {
        this.#status = Status.Connecting;
        this.emit(Events.Status, Status.Connecting);
        this.#innerConnect();
      }
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
  /** @type {amqplib.Channel|amqplib.ConfirmChannel} */
  #channel;
  /** @type {AmqpQueueMsgHandler} */
  #msgHandler;
  /** @type {OnConnStatusChanged} */
  #onConnStatusChangedBound;
}

module.exports = {
  AmqpQueue,
};
