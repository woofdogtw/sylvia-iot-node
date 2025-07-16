'use strict';

const http = require('./http');
const user = require('./user');
const callbackHttp = require('./callback/http');
const callbackUser = require('./callback/user');

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

    describe('callback/http', function () {
      it('new()', callbackHttp.testNew);
      it('new() with wrong options', callbackHttp.testNewWrong);
      it('request()', callbackHttp.testReq);
      it('request() with error', callbackHttp.testReqErr);
    });

    describe('callback/user', function () {
      it('get()', callbackUser.testGet);
      it('get() with error', callbackUser.testGetErr);
      it('update()', callbackUser.testUpdate);
      it('update() with error', callbackUser.testUpdateErr);
    });
  };
}

module.exports = {
  suite,
};
