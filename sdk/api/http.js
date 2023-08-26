'use strict';

const { Agent } = require('http');

const superagent = require('superagent');

const { DataTypes } = require('general-mq/lib/constants');

const keepAliveAgent = new Agent({ keepAlive: true });

/**
 * Options of the HTTP client `Client` that contains OAuth2 information.
 *
 * @typedef {Object} ClientOptions
 * @property {string} authBase `sylvia-iot-auth` base path with scheme. For example
 *           `http://localhost:1080/auth`.
 * @property {string} coremgrBase `sylvia-iot-coremgr` base path with scheme. For example
 *           `http://localhost:1080/coremgr`.
 * @property {string} clientId Client ID.
 * @property {string} clientSecret Client secret.
 */

/**
 * The HTTP client to request Sylvia-IoT APIs. With this client, you do not need to handle 401
 * refresh token flow.
 *
 * @class Client
 */
class Client {
  /**
   * Create an instance.
   *
   * @param {ClientOptions} opts
   * @throws {Error} Wrong arguments.
   */
  constructor(opts) {
    if (!opts || typeof opts !== DataTypes.Object || Array.isArray(opts)) {
      throw Error('`opts` is not an object');
    } else if (!opts.authBase || typeof opts.authBase !== DataTypes.String) {
      throw Error('`opts.authBase` is not a string');
    } else if (!opts.coremgrBase || typeof opts.coremgrBase !== DataTypes.String) {
      throw Error('`opts.coremgrBase` is not a string');
    } else if (!opts.clientId || typeof opts.clientId !== DataTypes.String) {
      throw Error('`opts.clientId` is not a string');
    } else if (!opts.clientSecret || typeof opts.clientSecret !== DataTypes.String) {
      throw Error('`opts.clientSecret` is not a string');
    }

    this.#authBase = opts.authBase;
    this.#coremgrBase = opts.coremgrBase;
    this.#clientId = opts.clientId;
    this.#clientSecret = opts.clientSecret;
  }

  /**
   * Execute a Sylvia-IoT API request.
   *
   * @param {string} method
   * @param {string} apiPath The relative path (of the coremgr base) the API with query string. For
   *        example: `/api/v1/user/list?contains=word`, the client will do a request with
            `http://coremgr-host/coremgr/api/v1/user/list?contains=word` URL.
   * @param {Object} [body]
   * @param {function} callback
   *   @param {?Error} callback.err
   *   @param {number} callback.status
   *   @param {Object|Array} callback.body
   * @throws {Error} Wrong arguments.
   */
  request(method, apiPath, body, callback) {
    if (!method || typeof method !== DataTypes.String) {
      throw Error('`method` is not a string');
    } else if (!apiPath || typeof apiPath !== DataTypes.String) {
      throw Error('`apiPath` is not a string');
    }
    if (typeof body === DataTypes.Function) {
      callback = body;
      body = undefined;
    }
    if (body !== undefined && (!body || typeof body !== DataTypes.Object)) {
      throw Error('`body` is not an object');
    }
    if (typeof callback !== DataTypes.Function) {
      throw Error('`callback` is not a function');
    }

    let retry = 1;
    let retStatus = 0;
    let retBody = null;
    const self = this;
    (function innerRequest() {
      if (retry < 0) {
        return void callback(null, retStatus, retBody);
      }

      if (!self.#accessToken) {
        return void self.#authToken((err, token) => {
          if (err) {
            return void callback(err);
          }
          self.#accessToken = token;
          retry--;
          innerRequest();
        });
      }

      superagent(method, `${self.#coremgrBase}${apiPath}`)
        .agent(keepAliveAgent)
        .set('Authorization', `Bearer ${self.#accessToken}`)
        .send(body)
        .buffer(false)
        .end((err, res) => {
          if (!res) {
            return void callback(err || Error(JSON.stringify({ code: 'err_rsc' })));
          }
          let body = res.body;
          if (res.body.length > 0) {
            // Try to parse JSON body.
            try {
              body = JSON.parse(res.body.toString());
            } catch (e) {}
          }

          retStatus = res.statusCode;
          retBody = body;
          if (res.statusCode === 401) {
            return void self.#authToken((err, token) => {
              if (err) {
                return void callback(err);
              }
              self.#accessToken = token;
              retry--;
              innerRequest();
            });
          }
          callback(null, retStatus, retBody);
        });
    })();
  }

  /**
   * To authorize the client and get access token/refresh token.
   *
   * @param {function} callback
   *   @param {?Error} callback.err
   *   @param {string} callback.token The access token.
   * @throws {Error} Wrong arguments.
   */
  #authToken(callback) {
    if (typeof callback !== DataTypes.Function) {
      throw Error('`callback` is not a function');
    }
    const url = `${this.#authBase}/oauth2/token`;
    const body = { grant_type: 'client_credentials' };
    superagent
      .agent(keepAliveAgent)
      .post(url)
      .auth(this.#clientId, this.#clientSecret)
      .type('form')
      .accept('application/json')
      .send(body)
      .redirects(0)
      .end((err, res) => {
        if (!res) {
          const body = {
            code: 'err_rsc',
            message: `${err}`,
          };
          return void callback(Error(JSON.stringify(body)));
        } else if (res.statusCode !== 200) {
          return void callback(Error(JSON.stringify(res.body)));
        }
        callback(null, res.body.access_token);
      });
  }

  /**
   * `sylvia-iot-auth` base path.
   *
   * @type {string}
   */
  #authBase;
  /**
   * `sylvia-iot-coremgr` base path.
   *
   * @type {string}
   */
  #coremgrBase;
  /**
   * Client ID.
   *
   * @type {string}
   */
  #clientId;
  /**
   * Client secret.
   *
   * @type {string}
   */
  #clientSecret;
  /**
   * The access token.
   *
   * @type {string}
   */
  #accessToken;
}

module.exports = {
  Client,
};
