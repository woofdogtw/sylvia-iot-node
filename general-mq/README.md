# general-mq

![npm](https://img.shields.io/npm/v/general-mq.svg?logo=npm)
[![Documentation](https://img.shields.io/badge/docs-ok.svg)](https://woofdogtw.github.io/sylvia-iot-node/apidocs/general-mq/)
![CI](https://github.com/woofdogtw/sylvia-iot-node/actions/workflows/build-test.yaml/badge.svg)
[![Coverage](https://raw.githubusercontent.com/woofdogtw/sylvia-iot-node/gh-pages/docs/coverage/general-mq/badge.svg)](https://woofdogtw.github.io/sylvia-iot-node/coverage/general-mq/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> This is the Node.js implementation (the original project is [**here**](https://github.com/woofdogtw/sylvia-iot-core/tree/main/general-mq)).

General purposed interfaces for message queues. Now we provide the following implementations:

- AMQP 0-9-1
- MQTT

By using these classes, you can configure queues with the following properties:

- Unicast or broadcast.
- Reliable or best-effort.

**Notes**

- MQTT uses **shared queues** to implement unicast.
- AMQP uses **confirm channels** to implement reliable publish, and MQTT uses **QoS 1** to
  implement reliable publish/subscribe.

# Relationships of Connections and Queues

The term **connection** describes a TCP/TLS connection to the message broker.
The term **queue** describes a message queue or a topic within a connection.
You can use one connection to manage multiple queues, or one connection to manage one queue.

A queue can only be a receiver or a sender at a time.

### Connections for sender/receiver queues with the same name

The sender and the receiver are usually different programs, there are two connections to hold two
queues.

For the special case that a program acts both the sender and the receiver using the same queue:

- The AMQP implementation uses one **Channel** for one queue, so the program can manages all
  queues with one connection.
- The MQTT implementation **MUST** uses one connection for one queue, or both sender and receiver
  will receive packets.

# Test

Please prepare a [RabbitMQ](https://www.rabbitmq.com/) broker and a [EMQX](https://emqx.io/)
broker at **localhost** for testing.

- To install using Docker:

      $ docker run --rm --name rabbitmq -d -p 5672:5672 rabbitmq:management-alpine
      $ docker run --rm --name emqx -d -p 1883:1883 emqx/emqx

Then run the test:

    $ npm run test

# Example

Launch RabbitMQ and then run AMQP example:

    $ node examples/simple.js

Launch EMQX and then run MQTT example:

    $ RUN_MQTT= node examples/simple.js
