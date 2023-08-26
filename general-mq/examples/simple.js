'use strict';

const async = require('async');

const gmq = require('..');
const { Events, Status } = gmq.constants;

const TEST_RELIABLE = true;
const TEST_BROADCAST = true;

if ('RUN_MQTT' in process.env) {
  tester(gmq.mqtt);
} else {
  tester(gmq.amqp);
}

function tester(engine) {
  const conn1 = new engine.Connection();
  conn1.on(Events.Status, onConnStatus);
  conn1.on(Events.Error, onConnError);

  let conn2 = conn1;
  if (engine === gmq.mqtt) {
    conn2 = new engine.Connection();
    conn2.on(Events.Status, onConnStatus);
    conn2.on(Events.Error, onConnError);
  }

  let optsSender = {
    name: 'test',
    isRecv: false,
    reliable: TEST_RELIABLE,
    broadcast: TEST_BROADCAST,
    reconnectTimeoutMillis: 1000,
    prefetch: 10,
  };
  const sendQueue = new engine.Queue(optsSender, conn1);
  sendQueue.on(Events.Status, genOnQueueStatus('send'));
  sendQueue.on(Events.Error, genOnQueueError('send'));
  sendQueue.connect();

  let opts = {
    name: 'test',
    isRecv: true,
    reliable: TEST_RELIABLE,
    broadcast: TEST_BROADCAST,
    reconnectTimeoutMillis: 1000,
    prefetch: 10,
    sharedPrefix: '$share/general-mq/',
  };
  const recvQueue1 = new engine.Queue(opts, conn1);
  recvQueue1.on(Events.Status, genOnQueueStatus('recv1'));
  recvQueue1.on(Events.Error, genOnQueueError('recv1'));
  recvQueue1.setMsgHandler(genMsgHandler('recv1'));
  recvQueue1.connect();
  const recvQueue2 = new engine.Queue(opts, conn2);
  recvQueue2.on(Events.Status, genOnQueueStatus('recv2'));
  recvQueue2.on(Events.Error, genOnQueueError('recv2'));
  recvQueue2.setMsgHandler(genMsgHandler('recv2'));
  recvQueue2.connect();

  const loopSend = function (count, cb) {
    if (count <= 0) {
      return void cb(null);
    }

    const str = `count ${count}`;
    sendQueue.sendMsg(Buffer.from(str), (err) => {
      if (err) {
        console.log(`send ${str} error: ${err}`);
      } else {
        console.log(`send ${str} ok`);
      }
      setTimeout(() => {
        loopSend(count - 1, cb);
      }, 2000);
    });
  };
  const loopFn = function () {
    async.waterfall(
      [
        function (cb) {
          conn1.connect();
          if (conn1 !== conn2) {
            conn2.connect();
          }
          loopSend(10, cb);
        },
        function (cb) {
          conn1.close((err) => {
            if (err) {
              console.log(`close 1 error: ${err}`);
              return void cb(err);
            }
            if (conn2 === conn1) {
              return void cb(null);
            }
            conn2.close((err) => {
              if (err) {
                console.log(`close 2 error: ${err}`);
                return void cb(err);
              }
              cb(null);
            });
          });
        },
      ],
      (err) => {
        if (err) {
          process.exit(0);
        }

        setTimeout(() => {
          loopFn();
        }, 5000);
      }
    );
  };
  loopFn();
}

function onConnStatus(status) {
  let str;
  switch (status) {
    case Status.Closing:
      str = 'status: closing';
      break;
    case Status.Closed:
      str = 'status: closed';
      break;
    case Status.Connecting:
      str = 'status: connecting';
      break;
    case Status.Connected:
      str = 'status: connected';
      break;
    case Status.Disconnected:
      str = 'status: disconnected';
      break;
    default:
      str = 'status: unknown';
      break;
  }
  console.log(`connection status: ${str}`);
}

function onConnError(err) {
  console.log(`connection error: ${err}`);
}

function genOnQueueStatus(name) {
  return function onQueueStatus(status) {
    let str;
    switch (status) {
      case Status.Closing:
        str = 'status: closing';
        break;
      case Status.Closed:
        str = 'status: closed';
        break;
      case Status.Connecting:
        str = 'status: connecting';
        break;
      case Status.Connected:
        str = 'status: connected';
        break;
      case Status.Disconnected:
        str = 'status: disconnected';
        break;
      default:
        str = 'status: unknown';
        break;
    }
    console.log(`queue ${name} status: ${str}`);
  };
}

function genOnQueueError(name) {
  return function onQueueError(err) {
    console.log(`queue ${name} error: ${err}`);
  };
}

function genMsgHandler(name) {
  return function msgHandler(queue, msg) {
    console.log(`name ${name} received ${Buffer.from(msg.payload).toString()}`);
    queue.ack(msg, (err) => {
      console.log(`name ${name} ack ` + (err ? `error: ${err}` : 'ok'));
    });
  };
}
