# node-kafka-adapter
Adapter to use Kafka for Udacity Services

## Maintainers
1. Brandon Truong
2. Dhruv Parthasarathy
3. Warren Ouyang

## Contributing
1. Use ESlint w/ 2 spaces
2. Docstrings using jsdocs
3. Make a branch and submit a PR!

## Set up

1. npm install
2. npm install -g mocha
3. `npm run build` after making changes (we should probably improve this)
4. `npm run test` to test


## Usage

### Create an adapter

```javascript
var KafkaAdapter = require('node-kafka-adapter').KafkaAdapter;

// initialize your adapter with a zookeeper connection string
var kafkaAdapter = new KafkaAdapter('zk-node-1:2181,zk-node-2:2181,zk-node-3:2181');
```

### Create topics
```javascript
kafkaAdapter.createTopics(contentTopic, request.request_topic).then(() => {});
```

### Subscribe to a topic

```javascript
// get an Observable of kafka messages for a given topic
// note: the stream dies after an error comes in. So call retry on it to have it restart
var responseStream = kafkaAdapter.createMessageStreamForTopic(request.request_topic, {autoCommit: false}).retry();

responseStream.subscribe(
  val => console.log('RESPONSE VAL', val),
  error => console.error('Subscription error', error)
);
```

### Write a message to a topic
```javascript
kafkaAdapter.writeMessageToTopic(JSON.stringify(myMessageObj), 'topic_name')
.catch(function (error) {
  console.error(error);
})
.then(function (res) {
  console.log('wrote', res);
});
```
