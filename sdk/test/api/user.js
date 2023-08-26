'use strict';

const assert = require('assert');

const { DataTypes } = require('general-mq/lib/constants');

const { Client, ClientOptions } = require('../../api/http');
const userapi = require('../../api/user');

const AUTH_BASE = 'http://127.0.0.1:1080/auth';
const COREMGR_BASE = 'http://127.0.0.1:1080/coremgr';
const CLIENT_ID = 'private';
const CLIENT_SECRET = 'secret';

// Pre-registered user/client in sylvia-iot-auth.
const ACCOUNT = 'admin';

function testGet(done) {
  /** @type {ClientOptions} */
  const opts = {
    authBase: AUTH_BASE,
    coremgrBase: COREMGR_BASE,
    clientId: CLIENT_ID,
    clientSecret: CLIENT_SECRET,
  };
  const client = new Client(opts);
  assert.ok(client);

  userapi.get(client, (err, data) => {
    if (err) {
      return void done(err);
    } else if (!data || typeof data !== DataTypes.Object) {
      return void done(Error('no data'));
    } else if (data.account !== ACCOUNT) {
      return void done(Error(`wrong account ${data.account} vs. ${ACCOUNT}`));
    } else if (!(data.createdAt instanceof Date) || isNaN(data.createdAt.getTime())) {
      return void done(Error('`createdAt` is not a Date'));
    } else if (!(data.modifiedAt instanceof Date) || isNaN(data.modifiedAt.getTime())) {
      return void done(Error('`modifiedAt` is not a Date'));
    }
    done(null);
  });
}

function testGetErr(done) {
  /** @type {ClientOptions} */
  const opts = {
    authBase: AUTH_BASE,
    coremgrBase: COREMGR_BASE,
    clientId: 'error',
    clientSecret: CLIENT_SECRET,
  };
  const client = new Client(opts);
  assert.ok(client);

  assert.throws(() => {
    userapi.get({});
  });
  assert.throws(() => {
    userapi.get(client, {});
  });

  userapi.get(client, (err, data) => {
    done(err ? null : Error(`sould error, data: ${JSON.stringify(data)}`));
  });
}

function testUpdate(done) {
  /** @type {ClientOptions} */
  const opts = {
    authBase: AUTH_BASE,
    coremgrBase: COREMGR_BASE,
    clientId: CLIENT_ID,
    clientSecret: CLIENT_SECRET,
  };
  const client = new Client(opts);
  assert.ok(client);

  userapi.update(client, { name: 'test' }, (err) => {
    done(err || null);
  });
}

function testUpdateErr(done) {
  /** @type {ClientOptions} */
  const opts = {
    authBase: AUTH_BASE,
    coremgrBase: COREMGR_BASE,
    clientId: 'error',
    clientSecret: CLIENT_SECRET,
  };
  const client = new Client(opts);
  assert.ok(client);

  assert.throws(() => {
    userapi.update({});
  });
  assert.throws(() => {
    userapi.update(client, null);
  });
  assert.throws(() => {
    userapi.update(client, {}, {});
  });

  userapi.update(client, { name: 'name' }, (err, data) => {
    done(err ? null : Error(`sould error, data: ${JSON.stringify(data)}`));
  });
}

module.exports = {
  testGet,
  testGetErr,
  testUpdate,
  testUpdateErr,
};
