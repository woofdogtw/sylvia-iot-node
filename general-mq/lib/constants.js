'use strict';

module.exports = {
  /**
   * Data types.
   *
   * @name DataTypes
   * @readonly
   * @enum {Symbol}
   */
  DataTypes: {
    /** @memberof DataTypes */
    Boolean: 'boolean',
    Function: 'function',
    Object: 'object',
    String: 'string',
  },
  /**
   * Errors.
   *
   * @name Errors
   * @readonly
   * @enum {string}
   */
  Errors: {
    NoMsgHandler: 'no message handler',
    NotConnected: 'not connected',
    QueueIsReceiver: 'this queue is a receiver',
  },
  /**
   * Events.
   *
   * @name Events
   * @readonly
   * @enum {string}
   */
  Events: {
    Error: 'error',
    Status: 'status',
  },
  QueuePattern: /^[a-z0-9_-]+([\.]{1}[a-z0-9_-]+)*$/,
  /**
   * Connection/Queue status.
   *
   * @name Status
   * @readonly
   * @enum {Symbol}
   */
  Status: {
    Closing: Symbol(),
    Closed: Symbol(),
    Connecting: Symbol(),
    Connected: Symbol(),
    Disconnected: Symbol(),
  },
};
