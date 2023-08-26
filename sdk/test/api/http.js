'use strict';

const assert = require('assert');

const async = require('async');

const { Client, ClientOptions } = require('../../api/http');

const AUTH_BASE = 'http://localhost:1080/auth';
const COREMGR_BASE = 'http://localhost:1080/coremgr';
const CLIENT_ID = 'private';
const CLIENT_SECRET = 'secret';

function testNew() {
  /** @type {ClientOptions} */
  const opts = {
    authBase: AUTH_BASE,
    coremgrBase: COREMGR_BASE,
    clientId: CLIENT_ID,
    clientSecret: CLIENT_SECRET,
  };
  assert.ok(new Client(opts));
}

function testNewWrong() {
  assert.throws(() => {
    new Client();
  });
  const opts = {
    authBase: 1,
  };
  assert.throws(() => {
    new Client(opts);
  });
  opts.authBase = AUTH_BASE;
  opts.coremgrBase = '';
  assert.throws(() => {
    new Client(opts);
  });
  opts.coremgrBase = COREMGR_BASE;
  opts.clientId = null;
  assert.throws(() => {
    new Client(opts);
  });
  opts.clientId = CLIENT_ID;
  assert.throws(() => {
    new Client(opts);
  });
}

function testReq(done) {
  /** @type {ClientOptions} */
  const opts = {
    authBase: AUTH_BASE,
    coremgrBase: COREMGR_BASE,
    clientId: CLIENT_ID,
    clientSecret: CLIENT_SECRET,
  };
  const client = new Client(opts);
  assert.ok(client);

  client.request('GET', '/api/v1/user', (err, status, body) => {
    if (err) {
      return void done(err);
    } else if (status !== 200) {
      return void done(body || status);
    }

    // Request twice to use in memory token.
    client.request('GET', '/api/v1/user', (err, status, body) => {
      if (err) {
        return void done(err);
      } else if (status !== 200) {
        return void done(body || status || {});
      }
      done(null);
    });
  });
}

function testReqErr(done) {
  /** @type {ClientOptions} */
  const opts = {
    authBase: AUTH_BASE,
    coremgrBase: COREMGR_BASE,
    clientId: CLIENT_ID,
    clientSecret: CLIENT_SECRET,
  };
  const client = new Client(opts);
  assert.ok(client);

  assert.throws(() => {
    client.request('');
  });
  assert.throws(() => {
    client.request('GET', '');
  });
  assert.throws(() => {
    client.request('GET', '/api', {});
  });
  assert.throws(() => {
    client.request('GET', '/api', {}, {});
  });
  assert.throws(() => {
    client.request('GET', '/api', null, () => {});
  });

  async.waterfall(
    [
      function (cb) {
        const opts = {
          authBase: 'http://localhost:1234',
          coremgrBase: COREMGR_BASE,
          clientId: CLIENT_ID,
          clientSecret: CLIENT_SECRET,
        };
        const client = new Client(opts);
        client.request('GET', '/api', (err) => {
          cb(err ? null : 'wrong authBase should error');
        });
      },
      function (cb) {
        const opts = {
          authBase: AUTH_BASE,
          coremgrBase: 'http://localhost:1234',
          clientId: CLIENT_ID,
          clientSecret: CLIENT_SECRET,
        };
        const client = new Client(opts);
        client.request('GET', '/api', (err) => {
          cb(err ? null : 'wrong coremgrBase should error');
        });
      },
    ],
    done
  );
}

module.exports = {
  testNew,
  testNewWrong,
  testReq,
  testReqErr,
};
