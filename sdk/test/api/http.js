'use strict';

const assert = require('assert');

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
  assert.throws(() => new Client());
  const opts = {
    authBase: 1,
  };
  assert.throws(() => new Client(opts));
  opts.authBase = AUTH_BASE;
  opts.coremgrBase = '';
  assert.throws(() => new Client(opts));
  opts.coremgrBase = COREMGR_BASE;
  opts.clientId = null;
  assert.throws(() => new Client(opts));
  opts.clientId = CLIENT_ID;
  assert.throws(() => new Client(opts));
}

async function testReq() {
  /** @type {ClientOptions} */
  const opts = {
    authBase: AUTH_BASE,
    coremgrBase: COREMGR_BASE,
    clientId: CLIENT_ID,
    clientSecret: CLIENT_SECRET,
  };
  const client = new Client(opts);
  assert.ok(client);

  let res = await client.request('GET', '/api/v1/user');
  assert.strictEqual(res.status, 200, res.body ? JSON.stringify(res.body) : `${res.status}`);

  // Request twice to use in memory token.
  res = await client.request('GET', '/api/v1/user');
  assert.strictEqual(res.status, 200, res.body ? JSON.stringify(res.body) : `${res.status}`);
}

async function testReqErr() {
  /** @type {ClientOptions} */
  let opts = {
    authBase: AUTH_BASE,
    coremgrBase: COREMGR_BASE,
    clientId: CLIENT_ID,
    clientSecret: CLIENT_SECRET,
  };
  let client = new Client(opts);
  assert.ok(client);

  const shouldErrFn = (res) => {
    throw Error(`should error, res: ${JSON.stringify(res)}`);
  };
  const catchFn = (err) => assert.ok(!(err instanceof SdkError));

  client.request('').then(shouldErrFn).catch(catchFn);
  client.request('GET', '').then(shouldErrFn).catch(catchFn);
  client.request('GET', '/api').then(shouldErrFn).catch(catchFn);
  client.request('GET', '/api', null).then(shouldErrFn).catch(catchFn);

  opts = {
    authBase: 'http://localhost:1234',
    coremgrBase: COREMGR_BASE,
    clientId: CLIENT_ID,
    clientSecret: CLIENT_SECRET,
  };
  client = new Client(opts);
  client.request('GET', '/api').then(shouldErrFn).catch(catchFn);

  opts = {
    authBase: AUTH_BASE,
    coremgrBase: 'http://localhost:1234',
    clientId: CLIENT_ID,
    clientSecret: CLIENT_SECRET,
  };
  client = new Client(opts);
  client.request('GET', '/api').then(shouldErrFn).catch(catchFn);
}

module.exports = {
  testNew,
  testNewWrong,
  testReq,
  testReqErr,
};
