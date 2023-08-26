'use strict';

const gmq = require('general-mq');

const appMgr = require('./application');
const netMgr = require('./network');
const { afterEachFn, removeRabbitmqQueues } = require('./lib');

function suite(engine) {
  return function () {
    describe('ApplicationMgr', function () {
      it('new() with default options', appMgr.newDefault(engine));
      it('new() with manual options', appMgr.newManual(engine));
      it('new() with wrong opts', appMgr.newWrongOpts(engine));
      it('close()', appMgr.close(engine));

      it('uldata', appMgr.uldata(engine));
      it('uldata with wrong content', appMgr.uldataWrong(engine));
      it('dldata', appMgr.dldata(engine));
      it('dldata with wrong content', appMgr.dldataWrong(engine));
      it('dldata-resp', appMgr.dldataResp(engine));
      it('dldata-result', appMgr.dldataResult(engine));

      afterEach(afterEachFn);
    });

    describe('NetworkMgr', function () {
      it('new() with default options', netMgr.newDefault(engine));
      it('new() with manual options', netMgr.newManual(engine));
      it('new() with wrong opts', netMgr.newWrongOpts(engine));
      it('close()', netMgr.close(engine));

      it('uldata', netMgr.uldata(engine));
      it('uldata with wrong content', netMgr.uldataWrong(engine));
      it('dldata', netMgr.dldata(engine));
      it('dldata with wrong content', netMgr.dldataWrong(engine));
      it('dldata-result', netMgr.dldataResult(engine));
      it('dldata-result with wrong content', netMgr.dldataResultWrong(engine));
      it('ctrl', netMgr.ctrl(engine));

      afterEach(afterEachFn);
    });

    if (engine === gmq.amqp) {
      after(removeRabbitmqQueues);
    }
  };
}

module.exports = {
  suite,
};
