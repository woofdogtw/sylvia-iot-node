# Sylvia-IoT Node.js SDK

![npm](https://img.shields.io/npm/v/sylvia-iot-sdk.svg?logo=npm)
[![Documentation](https://img.shields.io/badge/docs-ok.svg)](https://woofdogtw.github.io/sylvia-iot-node/apidocs/sdk/)
![CI](https://github.com/woofdogtw/sylvia-iot-node/actions/workflows/build-test.yaml/badge.svg)
[![Coverage](https://raw.githubusercontent.com/woofdogtw/sylvia-iot-node/gh-pages/docs/coverage/sdk/badge.svg)](https://woofdogtw.github.io/sylvia-iot-node/coverage/sdk/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> This is the Node.js implementation (the original project is [**here**](https://github.com/woofdogtw/sylvia-iot-core/tree/main/sylvia-iot-sdk)).

SDK for developing networks (adapters) and applications on Sylvia-IoT. The SDK contains:

- `api`: utilities for accessing Sylvia-IoT **coremgr** APIs.
- `middlewares`: middlewares.
  - `auth`: token authentication.
- `mq`: managers for managing network/application connections/queues by using `general-mq`.
