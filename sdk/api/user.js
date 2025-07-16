'use strict';

const { SdkError } = require('general-mq');
const { DataTypes } = require('general-mq/lib/constants');

const { ErrorCode, HttpMethod } = require('./constants');
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
 * @async
 * @param {Client} client
 * @param {function} callback
 * @returns {Promise<GetResData>}
 * @throws {Error} Wrong arguments.
 * @throws {SdkError}
 */
async function get(client) {
  if (!(client instanceof Client)) {
    throw Error('`client` is not a Client');
  }

  const res = await client.request(HttpMethod.Get, '/api/v1/user');
  if (res.status !== 200) {
    throw SdkError({ code: ErrorCode.Rsc, message: JSON.stringify(res.body) });
  }
  const data = res.body.data;
  data.createdAt = new Date(data.createdAt);
  data.modifiedAt = new Date(data.modifiedAt);
  if (data.verifiedAt) {
    data.verifiedAt = new Date(data.verifiedAt);
  }
  return data;
}

/**
 * `PATCH /coremgr/api/v1/user`
 *
 * @async
 * @param {Client} client
 * @param {PatchReqData} data
 * @throws {Error} Wrong arguments.
 * @throws {SdkError}
 */
async function update(client, data) {
  if (!(client instanceof Client)) {
    throw Error('`client` is not a Client');
  } else if (!data || typeof data !== DataTypes.Object || Array.isArray(data)) {
    throw Error('`data` is not an object');
  }

  const res = await client.request(HttpMethod.Patch, '/api/v1/user', { data });
  if (res.status !== 204) {
    return { code: ErrorCode.Rsc, message: JSON.stringify(body) };
  }
}

module.exports = {
  get,
  update,
};
