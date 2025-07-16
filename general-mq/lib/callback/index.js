'use strict';

const { AmqpConnection } = require('./amqp-connection');
const { AmqpQueue } = require('./amqp-queue');
const { MqttConnection } = require('./mqtt-connection');
const { MqttQueue } = require('./mqtt-queue');

/**
 * @typedef {Object} Engine
 * @property {AmqpConnection|MqttConnection} Connection
 * @property {AmqpQueue|MqttQueue} Queue
 */

module.exports = {
  /** @type {Engine} */
  amqp: {
    Connection: AmqpConnection,
    Queue: AmqpQueue,
  },
  /** @type {Engine} */
  mqtt: {
    Connection: MqttConnection,
    Queue: MqttQueue,
  },
  AmqpConnection,
  AmqpQueue,
  MqttConnection,
  MqttQueue,
};
