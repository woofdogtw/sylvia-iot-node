'use strict';

const { Agent } = require('http');

const superagent = require('superagent');

const { SdkError } = require('general-mq');
const { DataTypes } = require('general-mq/lib/constants');

const { ErrorCode } = require('./constants');

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
 * Client response.
 *
 * @typedef {Object} ClientResponse
 * @property {number} status Status code.
 * @property {Object|Array} body Body.
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
   * @async
   * @param {string} method
   * @param {string} apiPath The relative path (of the coremgr base) the API with query string. For
   *        example: `/api/v1/user/list?contains=word`, the client will do a request with
            `http://coremgr-host/coremgr/api/v1/user/list?contains=word` URL.
   * @param {Object} [body]
   * @returns {Promise<ClientResponse>}
   * @throws {Error} Wrong arguments.
   * @throws {SdkError}
   */
  async request(method, apiPath, body) {
    if (!method || typeof method !== DataTypes.String) {
      throw Error('`method` is not a string');
    } else if (!apiPath || typeof apiPath !== DataTypes.String) {
      throw Error('`apiPath` is not a string');
    } else if (body !== undefined && (!body || typeof body !== DataTypes.Object)) {
      throw Error('`body` is not an object');
    }

    if (!this.#accessToken) {
      this.#accessToken = await this.#authToken();
    }

    for (let retry = 1; retry >= 0; retry--) {
      const res = await superagent(method, `${this.#coremgrBase}${apiPath}`)
        .agent(keepAliveAgent)
        .set('Authorization', `Bearer ${this.#accessToken}`)
        .send(body)
        .buffer(false)
        .ok((res) => !!res)
        .catch((err) => {
          throw new SdkError({ code: ErrorCode.Rsc, message: `${err}` });
        });

      if (res.statusCode === 401) {
        this.#accessToken = await this.#authToken();
        continue;
      }

      let retBody = res.body;
      if (res.body.length > 0) {
        // Try to parse JSON body.
        try {
          retBody = JSON.parse(res.body.toString());
        } catch (e) {}
      }
      return {
        status: res.statusCode,
        body: retBody,
      };
    }
    throw new SdkError({
      code: ErrorCode.Rsc,
      message: 'exceed retry',
    });
  }

  /**
   * To authorize the client and get access token/refresh token.
   *
   * @async
   * @returns {Promise<string>} The access token.
   * @throws {Error} Wrong arguments.
   * @throws {SdkError}
   */
  async #authToken() {
    const url = `${this.#authBase}/oauth2/token`;
    const body = { grant_type: 'client_credentials' };
    const res = await superagent
      .agent(keepAliveAgent)
      .post(url)
      .auth(this.#clientId, this.#clientSecret)
      .type('form')
      .accept('application/json')
      .send(body)
      .ok((res) => !!res)
      .redirects(0)
      .catch((err) => {
        const body = {
          code: ErrorCode.Rsc,
          message: `${err}`,
        };
        throw new SdkError(JSON.stringify(body));
      });
    if (res.statusCode !== 200) {
      throw new SdkError(JSON.stringify(res.body));
    }
    return res.body.access_token;
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
