'use strict';

const { Agent } = require('http');

const superagent = require('superagent');

const gmq = require('..');
const conn = require('./common-connection');
const queue = require('./common-queue');

const engine = gmq.amqp;
const keepAliveAgent = new Agent({ keepAlive: true });

describe('amqp', function () {
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

function removeRabbitmqQueues(done) {
  function removeQueues(queues, callback) {
    const queue = queues.pop();
    if (!queue) {
      return void process.nextTick(() => {
        callback(null);
      });
    } else if (queue.name.startsWith('amq.')) {
      return void process.nextTick(() => {
        removeQueues(queues, callback);
      });
    }

    superagent
      .agent(keepAliveAgent)
      .auth('guest', 'guest')
      .delete(`http://localhost:15672/api/queues/%2f/${queue.name}`, (err, res) => {
        if (err) {
          return void callback(Error(`delete queue ${queue.name} error: ${err}`));
        } else if (res.statusCode !== 204 && res.statusCode !== 404) {
          return void callback(Error(`delete queues with status ${res.statusCode}`));
        }
        removeQueues(queues, callback);
      });
  }

  superagent
    .agent(keepAliveAgent)
    .auth('guest', 'guest')
    .get('http://localhost:15672/api/queues/%2f', (err, res) => {
      if (err) {
        return void done(Error(`get queue error: ${err}`));
      } else if (res.statusCode !== 200) {
        return void done(Error(`get queues with status ${res.statusCode}`));
      }
      removeQueues(res.body, done);
    });
}
