'use strict';

const { Agent } = require('http');
const querystring = require('querystring');
const { URL } = require('url');

const async = require('async');
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
const AUTH_URI = 'http://127.0.0.1:1080/auth/api/v1/auth/tokeninfo';
const AUTH_URI_BASE = 'http://127.0.0.1:1080/auth/oauth2';

let accessToken;

function beforeAll(done) {
  login(done);
}

function test200(done) {
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

  supertest(app)
    .get('/')
    .set('Authorization', `  bearer ${accessToken}  `)
    .end((err, res) => {
      if (err) {
        return void done(err);
      } else if (res.statusCode !== 204) {
        return void done(Error(`status ${res.statusCode}, body: ${JSON.stringify(res.body)}`));
      }
      done(null);
    });
}

function test400(done) {
  const app = express();
  app.get('/', auth.authMiddleware(AUTH_URI), (_req, res) => {
    res.status(204).end();
  });

  async.waterfall(
    [
      function (cb) {
        supertest(app)
          .get('/')
          .end((err, res) => {
            if (err) {
              return void cb(err);
            } else if (res.statusCode !== 400) {
              const msg = `[no auth] status ${res.statusCode}, body: ${JSON.stringify(res.body)}`;
              return void cb(Error(msg));
            }
            cb(null);
          });
      },
      function (cb) {
        supertest(app)
          .get('/')
          .set('Authorization', '')
          .end((err, res) => {
            if (err) {
              return void cb(err);
            } else if (res.statusCode !== 400) {
              const msg = `[auth empty] status ${res.statusCode}, body: ${JSON.stringify(
                res.body
              )}`;
              return void cb(Error(msg));
            }
            cb(null);
          });
      },
      function (cb) {
        supertest(app)
          .get('/')
          .set('Authorization', 'Basic 123')
          .end((err, res) => {
            if (err) {
              return void cb(err);
            } else if (res.statusCode !== 400) {
              const msg = `[auth basic] status ${res.statusCode}, body: ${JSON.stringify(
                res.body
              )}`;
              return void cb(Error(msg));
            }
            cb(null);
          });
      },
      function (cb) {
        supertest(app)
          .get('/')
          .set('Authorization', 'Bearer ')
          .end((err, res) => {
            if (err) {
              return void cb(err);
            } else if (res.statusCode !== 400) {
              const msg = `[auth bearer empty] status ${res.statusCode}, body: ${JSON.stringify(
                res.body
              )}`;
              return void cb(Error(msg));
            }
            cb(null);
          });
      },
    ],
    done
  );
}

function test401(done) {
  const app = express();
  app.get('/', auth.authMiddleware(AUTH_URI), (_req, res) => {
    res.status(204).end();
  });

  async.waterfall(
    [
      function (cb) {
        supertest(app)
          .get('/')
          .set('Authorization', 'Bearer test')
          .end((err, res) => {
            if (err) {
              return void cb(err);
            } else if (res.statusCode !== 401) {
              return void cb(Error(`status ${res.statusCode}, body: ${JSON.stringify(res.body)}`));
            }
            cb(null);
          });
      },
    ],
    done
  );
}

function test503(done) {
  const app = express();
  app.get('/', auth.authMiddleware('http://localhost:10811'), (_req, res) => {
    res.status(204).end();
  });

  async.waterfall(
    [
      function (cb) {
        supertest(app)
          .get('/')
          .set('Authorization', 'Bearer test')
          .end((err, res) => {
            if (err) {
              return void cb(err);
            } else if (res.statusCode !== 503) {
              return void cb(Error(`status ${res.statusCode}, body: ${JSON.stringify(res.body)}`));
            }
            cb(null);
          });
      },
    ],
    done
  );
}

/**
 * Log in the sylvia-iot-auth and get the access token.
 *
 * @param {function} callback
 *   @param {?Error} callback.err
 *   @param {string} [callback.token] The access token.
 */
function login(callback) {
  let sessionId;
  let authCode;

  async.waterfall(
    [
      // POST /login
      function (cb) {
        const stateValues = {
          response_type: 'code',
          client_id: CLIENT,
          redirect_uri: REDIRECT,
        };
        const body = {
          state: querystring.encode(stateValues),
          account: ACCOUNT,
          password: PASSWORD,
        };
        superagent
          .agent(keepAliveAgent)
          .post(AUTH_URI_BASE + '/login')
          .type('form')
          .accept('application/json')
          .send(body)
          .redirects(0)
          .end((_err, res) => {
            if (res.statusCode !== 302) {
              const body = JSON.stringify(res.body);
              return void cb(Error(`POST /login unexpected ${res.statusCode}, body: ${body}`));
            }
            let locHeader = res.get('location');
            if (locHeader.startsWith('/')) {
              locHeader = 'http://localhost' + locHeader;
            }
            const u = new URL(locHeader);
            const location = querystring.decode(u.search.replace('?', ''));
            sessionId = location.session_id;
            if (!sessionId) {
              return void cb(Error(`POST /login without session_id`));
            }
            cb(null);
          });
      },
      // POST /authorize
      function (cb) {
        const body = {
          response_type: 'code',
          client_id: CLIENT,
          redirect_uri: REDIRECT,
          allow: 'yes',
          session_id: sessionId,
        };
        superagent
          .agent(keepAliveAgent)
          .post(AUTH_URI_BASE + '/authorize')
          .type('form')
          .accept('application/json')
          .send(body)
          .redirects(0)
          .end((_err, res) => {
            if (res.statusCode !== 302) {
              const body = JSON.stringify(res.body);
              return void cb(Error(`POST /authorize unexpected ${res.statusCode}, body: ${body}`));
            }
            let locHeader = res.get('location');
            if (locHeader.startsWith('/')) {
              locHeader = 'http://localhost' + locHeader;
            }
            const u = new URL(locHeader);
            const location = querystring.decode(u.search.replace('?', ''));
            authCode = location.code;
            if (!authCode) {
              return void cb(Error(`POST /authorize without code`));
            }
            cb(null);
          });
      },
      // POST /token
      function (cb) {
        const body = {
          grant_type: 'authorization_code',
          code: authCode,
          client_id: CLIENT,
          redirect_uri: REDIRECT,
        };
        superagent
          .agent(keepAliveAgent)
          .post(AUTH_URI_BASE + '/token')
          .type('form')
          .accept('application/json')
          .send(body)
          .end((_err, res) => {
            if (res.statusCode !== 200) {
              return void cb(Error(`POST /token unexpected ${res.statusCode}, body: ${res.body}`));
            }
            accessToken = res.body.access_token;
            if (!accessToken) {
              return void cb(Error(`POST /authorize without access token`));
            }
            cb(null);
          });
      },
    ],
    callback
  );
}

module.exports = {
  beforeAll,
  test200,
  test400,
  test401,
  test503,
};
