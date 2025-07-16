'use strict';

const gmq = require('general-mq');
const callbackGmq = require('general-mq/lib/callback');

const api = require('./api');
const middlewares = require('./middlewares');
const mq = require('./mq');
const callbackMq = require('./mq/callback');

describe('Integration Test', function () {
  describe('api', api.suite());
  describe('middlewares', middlewares.suite());
  describe('mq - RabbitMQ', mq.suite(gmq.amqp));
  describe('mq - EMQX', mq.suite(gmq.mqtt));
  describe('mq/callback - RabbitMQ', callbackMq.suite(callbackGmq.amqp));
  describe('mq/callback - EMQX', callbackMq.suite(callbackGmq.mqtt));
});
