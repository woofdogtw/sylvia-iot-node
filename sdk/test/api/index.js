'use strict';

const http = require('./http');
const user = require('./user');

function suite() {
  return function () {
    describe('http', function () {
      it('new()', http.testNew);
      it('new() with wrong options', http.testNewWrong);
      it('request()', http.testReq);
      it('request() with error', http.testReqErr);
    });

    describe('user', function () {
      it('get()', user.testGet);
      it('get() with error', user.testGetErr);
      it('update()', user.testUpdate);
      it('update() with error', user.testUpdateErr);
    });
  };
}

module.exports = {
  suite,
};
