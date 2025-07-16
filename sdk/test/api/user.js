'use strict';

const assert = require('assert');

const { DataTypes } = require('general-mq').constants;

const { Client, ClientOptions } = require('../../api/http');
const userapi = require('../../api/user');

const AUTH_BASE = 'http://localhost:1080/auth';
const COREMGR_BASE = 'http://localhost:1080/coremgr';
const CLIENT_ID = 'private';
const CLIENT_SECRET = 'secret';

// Pre-registered user/client in sylvia-iot-auth.
const ACCOUNT = 'admin';

async function testGet() {
  /** @type {ClientOptions} */
  const opts = {
    authBase: AUTH_BASE,
    coremgrBase: COREMGR_BASE,
    clientId: CLIENT_ID,
    clientSecret: CLIENT_SECRET,
  };
  const client = new Client(opts);
  assert.ok(client);

  const data = await userapi.get(client);
  assert.ok(data && typeof data === DataTypes.Object, 'no data');
  assert.strictEqual(data.account, ACCOUNT, `wrong account ${data.account} vs. ${ACCOUNT}`);
  assert.ok(
    data.createdAt instanceof Date && !isNaN(data.createdAt.getTime()),
    '`createdAt` is not a Date'
  );
  assert.ok(
    data.modifiedAt instanceof Date && !isNaN(data.modifiedAt.getTime()),
    '`modifiedAt` is not a Date'
  );
}

async function testGetErr() {
  /** @type {ClientOptions} */
  const opts = {
    authBase: AUTH_BASE,
    coremgrBase: COREMGR_BASE,
    clientId: 'error',
    clientSecret: CLIENT_SECRET,
  };
  const client = new Client(opts);
  assert.ok(client);

  const shouldErrFn = (data) => {
    throw Error(`should error, data: ${JSON.stringify(data)}`);
  };
  const catchFn = (err) => assert.ok(!(err instanceof SdkError));

  userapi.get({}).then(shouldErrFn).catch(catchFn);
  userapi.get(client).then(shouldErrFn).catch(catchFn);
}

async function testUpdate() {
  /** @type {ClientOptions} */
  const opts = {
    authBase: AUTH_BASE,
    coremgrBase: COREMGR_BASE,
    clientId: CLIENT_ID,
    clientSecret: CLIENT_SECRET,
  };
  const client = new Client(opts);
  assert.ok(client);

  await userapi.update(client, { name: 'test' });
}

async function testUpdateErr() {
  /** @type {ClientOptions} */
  const opts = {
    authBase: AUTH_BASE,
    coremgrBase: COREMGR_BASE,
    clientId: 'error',
    clientSecret: CLIENT_SECRET,
  };
  const client = new Client(opts);
  assert.ok(client);

  const shouldErrFn = (data) => {
    throw Error(`should error, data: ${JSON.stringify(data)}`);
  };
  const catchFn = (err) => assert.ok(!(err instanceof SdkError));

  userapi.update({}).then(shouldErrFn).catch(catchFn);
  userapi.update(client, null).then(shouldErrFn).catch(catchFn);
  userapi.update(client, { name: 'name' }).then(shouldErrFn).catch(catchFn);
}

module.exports = {
  testGet,
  testGetErr,
  testUpdate,
  testUpdateErr,
};
