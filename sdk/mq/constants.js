'use strict';

module.exports = {
  /**
   * Control message operations.
   *
   * @name CtrlOp
   * @readonly
   * @enum {string}
   */
  CtrlOp: {
    AddDevice: 'add-device',
    AddDeviceBulk: 'add-device-bulk',
    AddDeviceRange: 'add-device-range',
    DelDevice: 'del-device',
    DelDeviceBulk: 'del-device-bulk',
    DelDeviceRange: 'del-device-range',
  },
  /**
   * Errors.
   *
   * @name Errors
   * @readonly
   * @enum {string}
   */
  Errors: {
    ErrParamCorrId: 'the `correlationId` must be a non-empty string',
    ErrParamDev:
      'one of `device_id` or [`network_code`, `network_addr`] pair must be provided with non-empty string',
  },
  /**
   * ApplicationMgr/NetworkMgr status.
   *
   * @name MgrStatus
   * @readonly
   * @enum {Symbol}
   */
  MgrStatus: {
    NotReady: Symbol(),
    Ready: Symbol(),
  },
  /**
   * Support application/network host schemes.
   */
  SupportSchemes: ['amqp', 'amqps', 'mqtt', 'mqtts'],
};
