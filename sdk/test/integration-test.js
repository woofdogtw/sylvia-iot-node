'use strict';

const gmq = require('general-mq');

const api = require('./api');
const middlewares = require('./middlewares');
const mq = require('./mq');

describe('Integration Test', function () {
  describe('api', api.suite());
  describe('middlewares', middlewares.suite());
  describe('mq - RabbitMQ', mq.suite(gmq.amqp));
  describe('mq - EMQX', mq.suite(gmq.mqtt));
});
