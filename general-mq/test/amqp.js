'use strict';

const { Agent } = require('http');

const superagent = require('superagent');

describe('amqp', function () {
  const gmq = require('..');
  const conn = require('./common-connection');
  const queue = require('./common-queue');
  const engine = gmq.amqp;

  describe('AmqpConnection', function () {
    it('new with default', conn.newDefault(engine));
    it('new with zero', conn.newZero(engine));
    it('new with wrong opts', conn.newWrongOpts(engine));

    it('status()', conn.properties(engine));

    it('connect() without handler', conn.connectNoHandler(engine));
    it('connect() with handler', conn.connectWithHandler(engine));
    it('connect() after connect()', conn.connectAfterConnect(engine));
    it.skip('connect() with insecure', conn.connectInsecure(engine));

    it('close()', conn.close(engine));
    it('close() after close()', conn.closeAfterClose(engine));
    it('close() without await', conn.closeNoAwait(engine));

    afterEach(conn.afterEach);
  });

  describe('AmqpQueue', function () {
    it('new with default', queue.newDefault(engine));
    it('new with zero', queue.newZero(engine));
    it('new with wrong opts', queue.newWrongOpts(engine));

    it('status()', queue.properties(engine));

    it('connect() without handler', queue.connectNoHandler(engine));
    it('connect() with handler', queue.connectWithHandler(engine));
    it('connect() after connect()', queue.connectAfterConnect(engine));

    it('setMsgHandler()', queue.setMsgHandler(engine));

    it('close()', queue.close(engine));
    it('close() after close()', queue.closeAfterClose(engine));

    it('sendMsg() with error conditions', queue.sendError(engine));

    afterEach(queue.afterEach);
    after(removeRabbitmqQueues);
  });

  describe('Senarios', function () {
    it('reconnect', queue.reconnect(engine));

    it('unicast 1 to 1', queue.dataUnicast1to1(engine));
    it('unicast 1 to 3', queue.dataUnicast1to3(engine));

    it('broadcast 1 to 1', queue.dataBroadcast1to1(engine));
    it('broadcast 1 to 3', queue.dataBroadcast1to3(engine));

    it('reliable', queue.dataReliable(engine));
    it('best effort', queue.dataBestEffort(engine));

    it('persistent', queue.dataPersistent(engine));
    it('nack', queue.dataNack(engine));
    it('ack/nack with wrong parameters', queue.dataAckNackWrong(engine));

    afterEach(queue.afterEach);
    after(removeRabbitmqQueues);
  });
});

describe('callback/amqp', function () {
  const gmq = require('../lib/callback');
  const conn = require('./callback/common-connection');
  const queue = require('./callback/common-queue');
  const engine = gmq.amqp;

  describe('AmqpConnection', function () {
    it('new with default', conn.newDefault(engine));
    it('new with zero', conn.newZero(engine));
    it('new with wrong opts', conn.newWrongOpts(engine));

    it('status()', conn.properties(engine));

    it('connect() without handler', conn.connectNoHandler(engine));
    it('connect() with handler', conn.connectWithHandler(engine));
    it('connect() after connect()', conn.connectAfterConnect(engine));
    it.skip('connect() with insecure', conn.connectInsecure(engine));

    it('close()', conn.close(engine));
    it('close() after close()', conn.closeAfterClose(engine));
    it('close() without callback', conn.closeNoCallback(engine));

    afterEach(conn.afterEach);
  });

  describe('AmqpQueue', function () {
    it('new with default', queue.newDefault(engine));
    it('new with zero', queue.newZero(engine));
    it('new with wrong opts', queue.newWrongOpts(engine));

    it('status()', queue.properties(engine));

    it('connect() without handler', queue.connectNoHandler(engine));
    it('connect() with handler', queue.connectWithHandler(engine));
    it('connect() after connect()', queue.connectAfterConnect(engine));

    it('setMsgHandler()', queue.setMsgHandler(engine));

    it('close()', queue.close(engine));
    it('close() after close()', queue.closeAfterClose(engine));

    it('sendMsg() with error conditions', queue.sendError(engine));

    afterEach(queue.afterEach);
    after(removeRabbitmqQueues);
  });

  describe('Senarios', function () {
    it('reconnect', queue.reconnect(engine));

    it('unicast 1 to 1', queue.dataUnicast1to1(engine));
    it('unicast 1 to 3', queue.dataUnicast1to3(engine));

    it('broadcast 1 to 1', queue.dataBroadcast1to1(engine));
    it('broadcast 1 to 3', queue.dataBroadcast1to3(engine));

    it('reliable', queue.dataReliable(engine));
    it('best effort', queue.dataBestEffort(engine));

    it('persistent', queue.dataPersistent(engine));
    it('nack', queue.dataNack(engine));
    it('ack/nack with wrong parameters', queue.dataAckNackWrong(engine));

    afterEach(queue.afterEach);
    after(removeRabbitmqQueues);
  });
});

async function removeRabbitmqQueues() {
  const keepAliveAgent = new Agent({ keepAlive: true });

  const res = await superagent
    .agent(keepAliveAgent)
    .auth('guest', 'guest')
    .get('http://localhost:15672/api/queues/%2f');

  if (res.statusCode !== 200) {
    throw Error(`get queues with status ${res.statusCode}`);
  }

  const queues = res.body.filter((queue) => !queue.name.startsWith('amq.'));

  for (const queue of queues) {
    const deleteRes = await superagent
      .agent(keepAliveAgent)
      .auth('guest', 'guest')
      .delete(`http://localhost:15672/api/queues/%2f/${queue.name}`);

    if (deleteRes.statusCode !== 204 && deleteRes.statusCode !== 404) {
      throw Error(`delete queue ${queue.name} with status ${deleteRes.statusCode}`);
    }
  }
}
