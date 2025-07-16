'use strict';

const assert = require('assert');
const { Agent } = require('http');
const querystring = require('querystring');
const { URL } = require('url');

const express = require('express');
const superagent = require('superagent');
const supertest = require('supertest');

const auth = require('../../middlewares/auth');

const keepAliveAgent = new Agent({ keepAlive: true });

// Pre-registered user/client in sylvia-iot-auth.
const USER_ID = 'admin';
const ACCOUNT = 'admin';
const PASSWORD = 'admin';
const CLIENT = 'public';
const REDIRECT = 'http://localhost:1080/auth/oauth2/redirect';

// API information.
const AUTH_URI = 'http://localhost:1080/auth/api/v1/auth/tokeninfo';
const AUTH_URI_BASE = 'http://localhost:1080/auth/oauth2';

let accessToken;

async function beforeAll() {
  await login();
}

async function test200() {
  const app = express();
  app.get('/', auth.authMiddleware(AUTH_URI), (req, res) => {
    /** @type {auth.FullTokenInfo} */
    const tokenInfo = req[auth.TokenInfoKey];
    if (!tokenInfo) {
      return void res.status(400).json({ message: '!tokenInfo' });
    } else if (tokenInfo.token !== accessToken) {
      return void res.status(400).json({ message: '!token' });
    } else if (tokenInfo.info.userId !== USER_ID) {
      return void res.status(400).json({ message: '!userId' });
    } else if (tokenInfo.info.account !== ACCOUNT) {
      return void res.status(400).json({ message: '!account' });
    } else if (tokenInfo.info.clientId !== CLIENT) {
      return void res.status(400).json({ message: '!clientId' });
    }
    res.status(204).end();
  });

  const res = await supertest(app).get('/').set('Authorization', `  bearer ${accessToken}  `);
  assert.strictEqual(
    res.statusCode,
    204,
    `status ${res.statusCode}, body: ${JSON.stringify(res.body)}`
  );
}

async function test400() {
  const app = express();
  app.get('/', auth.authMiddleware(AUTH_URI), (_req, res) => res.status(204).end());

  let res = await supertest(app).get('/');
  assert.strictEqual(
    res.statusCode,
    400,
    `[no auth] status ${res.statusCode}, body: ${JSON.stringify(res.body)}`
  );
  res = await supertest(app).get('/').set('Authorization', '');
  assert.strictEqual(
    res.statusCode,
    400,
    `[auth empty] status ${res.statusCode}, body: ${JSON.stringify(res.body)}`
  );
  res = await supertest(app).get('/').set('Authorization', 'Basic 123');
  assert.strictEqual(
    res.statusCode,
    400,
    `[auth basic] status ${res.statusCode}, body: ${JSON.stringify(res.body)}`
  );
  res = await supertest(app).get('/').set('Authorization', 'Basic ');
  assert.strictEqual(
    res.statusCode,
    400,
    `[auth bearer empty] status ${res.statusCode}, body: ${JSON.stringify(res.body)}`
  );
}

async function test401() {
  const app = express();
  app.get('/', auth.authMiddleware(AUTH_URI), (_req, res) => res.status(204).end());

  const res = await supertest(app).get('/').set('Authorization', 'Bearer test');
  assert.strictEqual(
    res.statusCode,
    401,
    `status ${res.statusCode}, body: ${JSON.stringify(res.body)}`
  );
}

async function test503() {
  const app = express();
  app.get('/', auth.authMiddleware('http://localhost:10811'), (_req, res) => res.status(204).end());

  const res = await supertest(app).get('/').set('Authorization', 'Bearer test');
  assert.strictEqual(
    res.statusCode,
    503,
    `status ${res.statusCode}, body: ${JSON.stringify(res.body)}`
  );
}

/**
 * Log in the sylvia-iot-auth and get the access token.
 *
 * @async
 * @throws {Error}
 */
async function login() {
  // POST /login
  const stateValues = {
    response_type: 'code',
    client_id: CLIENT,
    redirect_uri: REDIRECT,
  };
  let body = {
    state: querystring.encode(stateValues),
    account: ACCOUNT,
    password: PASSWORD,
  };
  let res = await superagent
    .agent(keepAliveAgent)
    .post(AUTH_URI_BASE + '/login')
    .type('form')
    .accept('application/json')
    .send(body)
    .ok((res) => !!res)
    .redirects(0);
  assert.strictEqual(
    res.statusCode,
    302,
    `POST /login unexpected ${res.statusCode}, body: ${JSON.stringify(res.body)}`
  );
  let locHeader = res.get('location');
  if (locHeader.startsWith('/')) {
    locHeader = 'http://localhost' + locHeader;
  }
  let u = new URL(locHeader);
  let location = querystring.decode(u.search.replace('?', ''));
  const sessionId = location.session_id;
  assert.ok(sessionId, 'POST /login without session_id');

  // POST /authorize
  body = {
    response_type: 'code',
    client_id: CLIENT,
    redirect_uri: REDIRECT,
    allow: 'yes',
    session_id: sessionId,
  };
  res = await superagent
    .agent(keepAliveAgent)
    .post(AUTH_URI_BASE + '/authorize')
    .type('form')
    .accept('application/json')
    .send(body)
    .ok((res) => !!res)
    .redirects(0);
  assert.strictEqual(
    res.statusCode,
    302,
    `POST /authorize unexpected ${res.statusCode}, body: ${JSON.stringify(res.body)}`
  );
  locHeader = res.get('location');
  if (locHeader.startsWith('/')) {
    locHeader = 'http://localhost' + locHeader;
  }
  u = new URL(locHeader);
  location = querystring.decode(u.search.replace('?', ''));
  const authCode = location.code;
  assert.ok(authCode, 'POST /authorize without code');

  // POST /token
  body = {
    grant_type: 'authorization_code',
    code: authCode,
    client_id: CLIENT,
    redirect_uri: REDIRECT,
  };
  res = await superagent
    .agent(keepAliveAgent)
    .post(AUTH_URI_BASE + '/token')
    .type('form')
    .accept('application/json')
    .send(body)
    .ok((res) => !!res);
  assert.strictEqual(
    res.statusCode,
    200,
    `POST /token unexpected ${res.statusCode}, body: ${JSON.stringify(res.body)}`
  );
  accessToken = res.body.access_token;
  assert.ok(accessToken, 'POST /authorize without access token');
}

module.exports = {
  beforeAll,
  test200,
  test400,
  test401,
  test503,
};
