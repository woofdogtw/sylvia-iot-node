'use strict';

const auth = require('./auth');

function suite() {
  return function () {
    describe('auth', function () {
      before(auth.beforeAll);

      it('200', auth.test200);
      it('400', auth.test400);
      it('401', auth.test401);
      it('503', auth.test503);
    });
  };
}

module.exports = {
  suite,
};
