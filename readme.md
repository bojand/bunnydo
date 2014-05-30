# bunnydo

Wrapper around [amqplib](https://github.com/squaremo/amqp.node) to make common patterns easier.

Still work in progress. Will document more thoroughly soon. Might change and expand.

Basic usage:

```js
var Bunnydo = require('bunnydo');

var amqp = new Bunnydo('amqp://localhost'');
amqp.init(function (err) {

  amqp.worker('work_queue', 'something to do', function (err) {
    // sent to work queue
  });

  amqp.rpc('rpc_queue', 'something to do', function (err, rpcRes) {
    console.dir(rpcRes); // response from RPC worker
  });

  amqp.pubsub('pubsub_queue', 'something to do', function (err, rpcRes) {
    // send to pubsub
  });
});



```

Then on the other end:

```js
var Bunnydo = require('bunnydo');

var amqp = new Bunnydo('amqp://localhost'');
amqp.init(function (err) {

  amqp.onWorker('work_queue', function (err, msg) {
    console.log("Worker received '%s'", msg);
  });

  amqp.onRpc('rpc_queue', function (err, msg, replyFn) {
    console.log("RPC received '%s'", msg);
    replyFn(msg.toUpperCase());
  });

  amqp.onPubsub('pubsub_queue', function (err, msg) {
    console.log("pubsub received '%s'", msg);
  });

});
```

Examples of use can be seen [here](https://github.com/bojand/bunny-test).