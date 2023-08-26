'use strict';

const { DataTypes } = require('general-mq/lib/constants');

const { Client } = require('./http');

/**
 * @typedef {Object} GetResData
 * @property {string} [userId]
 * @property {string} account
 * @property {Date} createdAt
 * @property {Date} modifiedAt
 * @property {Date} [verifiedAt]
 * @property {Object} roles  <string, boolean> pairs.
 * @property {string} name
 * @property {Object} info
 */

/**
 * @typedef {Object} PatchReqData
 * @property {string} [password]
 * @property {string} [name]
 * @property {Object} [info]
 */

/**
 * `GET /coremgr/api/v1/user`.
 *
 * @param {Client} client
 * @param {function} callback
 *   @param {?Error} callback.err
 *   @param {GetResData} callback.data
 * @throws {Error} Wrong arguments.
 */
function get(client, callback) {
  if (!(client instanceof Client)) {
    throw Error('`client` is not a Client');
  } else if (typeof callback !== DataTypes.Function) {
    throw Error('`callback` is not a function');
  }

  client.request('GET', '/api/v1/user', (err, status, body) => {
    if (err) {
      return void callback(err);
    } else if (status !== 200) {
      return void callback(Error({ code: 'err_rsc', message: JSON.stringify(body) }));
    }
    const data = body.data;
    data.createdAt = new Date(data.createdAt);
    data.modifiedAt = new Date(data.modifiedAt);
    if (data.verifiedAt) {
      data.verifiedAt = new Date(data.verifiedAt);
    }
    callback(null, data);
  });
}

/**
 * `PATCH /coremgr/api/v1/user`
 *
 * @param {Client} client
 * @param {PatchReqData} data
 * @param {function} callback
 *   @param {?Error} callback.err
 * @throws {Error} Wrong arguments.
 */
function update(client, data, callback) {
  if (!(client instanceof Client)) {
    throw Error('`client` is not a Client');
  } else if (!data || typeof data !== DataTypes.Object || Array.isArray(data)) {
    throw Error('`data` is not an object');
  } else if (typeof callback !== DataTypes.Function) {
    throw Error('`callback` is not a function');
  }

  client.request('PATCH', '/api/v1/user', { data }, (err, status, body) => {
    if (err) {
      return void callback(err);
    }
    callback(status === 204 ? null : Error({ code: 'err_rsc', message: JSON.stringify(body) }));
  });
}

module.exports = {
  get,
  update,
};
