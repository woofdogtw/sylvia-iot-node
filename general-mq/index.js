'use strict';

const { AmqpConnection } = require('./lib/amqp-connection');
const { AmqpQueue } = require('./lib/amqp-queue');
const constants = require('./lib/constants');
const { MqttConnection } = require('./lib/mqtt-connection');
const { MqttQueue } = require('./lib/mqtt-queue');

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
  constants,
  /** @type {Engine} */
  mqtt: {
    Connection: MqttConnection,
    Queue: MqttQueue,
  },
};
