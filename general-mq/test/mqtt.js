'use strict';

const assert = require('assert');

const gmq = require('..');
const conn = require('./common-connection');
const queue = require('./common-queue');

const engine = gmq.mqtt;

describe('mqtt', function () {
  describe('MqttConnection', function () {
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

    it('addPacketHandler() with wrong parameters', addPacketHandlerWrong);
    it('removePacketHandler() with wrong parameters', removePacketHandlerWrong);

    afterEach(conn.afterEach);
  });

  describe('MqttQueue', function () {
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
  });

  describe('Senarios', function () {
    it('reconnect', queue.reconnect(engine));
    it('unicast 1 to 1', queue.dataUnicast1to1(engine));
    it('unicast 1 to 3', queue.dataUnicast1to3(engine));

    it('broadcast 1 to 1', queue.dataBroadcast1to1(engine));
    it('broadcast 1 to 3', queue.dataBroadcast1to3(engine));

    xit('reliable', queue.dataReliable(engine));
    it('best effort', queue.dataBestEffort(engine));

    it('ack/nack with wrong parameters', queue.dataAckNackWrong(engine));

    afterEach(queue.afterEach);
  });
});

function addPacketHandlerWrong() {
  const conn = new engine.Connection();
  assert.ok(conn);
  assert.throws(() => {
    conn.addPacketHandler('A@');
  });
  assert.throws(() => {
    conn.addPacketHandler('name', 'name1');
  });
  assert.throws(() => {
    conn.addPacketHandler('name', 'name', 0);
  });
  assert.throws(() => {
    conn.addPacketHandler('name', 'name', false, {});
  });
}

function removePacketHandlerWrong() {
  const conn = new engine.Connection();
  assert.ok(conn);
  assert.throws(() => {
    conn.removePacketHandler('A@');
  });
}
