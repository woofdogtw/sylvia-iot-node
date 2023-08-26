'use strict';

const { Agent, ClientRequest, ServerResponse } = require('http');

const superagent = require('superagent');

const keepAliveAgent = new Agent({ keepAlive: true });

/**
 * @typedef {Object} FullTokenInfo
 * @property {string} token The access token.
 * @property {Object} info
 *   @property {string} info.userId
 *   @property {string} info.account
 *   @property {Object} info.roles <string, boolean> pairs.
 *   @property {string} info.name
 *   @property {string} info.clientId
 *   @property {string[]} info.scopes
 */

/**
 * Generate the Express authentication middleware.
 *
 * @param {string} authUri The authentication endpoint. For example
 *        `http://localhost:1080/auth/api/v1/auth/tokeninfo`.
 * @returns {function} The Express middleware.
 */
function authMiddleware(authUri) {
  /**
   * @param {ClientRequest} req
   * @param {ServerResponse} res
   * @param {function} next
   */
  return function (req, res, next) {
    let token = req.get('Authorization');
    if (!token) {
      return void res.status(400).json({ code: 'err_param', message: 'empty Authorization' });
    }
    token = token.trim();
    if (token.length < 8 || token.substr(0, 7).toLowerCase() !== 'bearer ') {
      return void res.status(400).json({ code: 'err_param', message: 'not bearer token' });
    }

    superagent
      .agent(keepAliveAgent)
      .auth(token.substr(7), { type: 'bearer' })
      .get(authUri, (err, authRes) => {
        if (!authRes) {
          return void res.status(503).json({
            code: 'err_rsc',
            message: `${err}`,
          });
        } else if (authRes.statusCode === 401) {
          return void res.status(401).json({ code: 'err_auth' });
        } else if (authRes.statusCode !== 200) {
          return void res.status(503).json({
            code: 'err_int_msg',
            message: `auth error with status code: ${res.statusCode}`,
          });
        }

        req[module.exports.TokenInfoKey] = {
          token: token.substr(7),
          info: authRes.body.data,
        };
        next();
      });
  };
}

module.exports = {
  TokenInfoKey: 'FullTokenInfo',
  authMiddleware,
};
