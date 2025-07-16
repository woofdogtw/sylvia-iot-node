'use strict';

/**
 * The general purposed error type for SDK errors.
 */
class SdkError extends Error {
  constructor(message) {
    super(message);
    this.name = 'SdkError';
  }
}

module.exports = {
  SdkError,
};
