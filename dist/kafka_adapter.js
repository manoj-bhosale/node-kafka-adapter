'use strict';

Object.defineProperty(exports, '__esModule', {
  value: true
});

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

function _interopRequireWildcard(obj) { if (obj && obj.__esModule) { return obj; } else { var newObj = {}; if (obj != null) { for (var key in obj) { if (Object.prototype.hasOwnProperty.call(obj, key)) newObj[key] = obj[key]; } } newObj['default'] = obj; return newObj; } }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

var _bluebird = require('bluebird');

var _kafkaNode = require('kafka-node');

var kafka = _interopRequireWildcard(_kafkaNode);

var _lodash = require('lodash');

var _ = _interopRequireWildcard(_lodash);

var _rx = require('rx');

var producerPromise;

/**
* Create an Observable stream from a kafka consumer
*/
var createStreamFromConsumer = function createStreamFromConsumer(kafkaConsumer) {
  return _rx.Observable.create(function (observer) {
    kafkaConsumer.on('message', function (message) {
      return observer.onNext(message.value);
    });
    kafkaConsumer.on('error', function (error) {
      return observer.onError(error);
    });
    kafkaConsumer.on('offsetOutOfRange', function (topic) {
      return observer.onError(topic);
    });
  });
};

var KafkaAdapter = (function () {
  function KafkaAdapter(zookeeperAddress) {
    _classCallCheck(this, KafkaAdapter);

    this.zookeeperAddress = zookeeperAddress;
  }

  /**
  * Create a new Kafka Producer or returns a promise for an existing one
  * Sets the producer promise to the output of this (we should only have one producer)
  * 
  * @returns: Promise with the producer. Resolves when the producer is ready. <Promise<kafka.Producer>>
  */

  _createClass(KafkaAdapter, [{
    key: 'initializeProducer',
    value: function initializeProducer() {
      var producerClientId = 'producer-' + Math.floor(Math.random() * 10000);
      var client = new kafka.Client(this.zookeeperAddress, producerClientId);
      var producer = new kafka.Producer(client);

      producerPromise = producerPromise || new _bluebird.Promise(function (resolve, reject) {
        producer.on('ready', function () {
          resolve(producer);
        });
        producer.on('error', function (err) {
          reject(err);
        });
      });
      return producerPromise;
    }

    /**
    * Create an Observable stream for a given topic
    * @param {string} topic The topic name
    * @param {object} optional config to pass in for the consumer
     *@returns {Observable} 
    */
  }, {
    key: 'createMessageStreamForTopic',
    value: function createMessageStreamForTopic(topic, configs) {
      var clientId = 'worker-' + Math.floor(Math.random() * 10000);
      var consumerClient = new kafka.Client(this.zookeeperAddress, clientId);
      var topicConsumer = new kafka.HighLevelConsumer(consumerClient, [{ topic: topic }], configs || {});
      return createStreamFromConsumer(topicConsumer);
    }

    /**
    * is the message a request?
    * @param{string} message js object representing Kafka message payload
    */
  }, {
    key: 'createMessageResponseObject',

    /**
    * generate an object representing a response to write back to kafka
    * @param {object: {data: [key:string]: any}} response - the response to the request
    * @param {object: {correlation_id: string, response_topic: string}} request
    * @returns {object: {response: correlation_id: errors?:}}
    */
    value: function createMessageResponseObject(response, request) {
      var messageResponseObject = {
        errors: [],
        response: response
      };

      if (!request.correlation_id) {
        // return an error response
        messageResponseObject.errors.concat(['Invalid Request: Request is missing correlation_id. Request is ' + request]);
      } else {
        messageResponseObject.correlation_id = request.correlation_id;
      }

      if (!request.request_id) {
        messageResponseObject.errors.concat(['Invalid Request: Request is missing request_id. Request is ' + request]);
      } else {
        messageResponseObject.request_id = request.request_id;
      }

      return messageResponseObject;
    }

    /**
    * write the given response body to the given kafka topic for the request w/ the correlation_id
    * @param {object} response - js object representing json response
    * @param {object: {correlation_id: string, response_topic: string, body: string}} request - js object representing request message on kafka
    * @returns {Promise<string>} Kafka's response to sending the message
    * @throws {Error} - if the request doesn't have a response_topic, throw an error
    */
  }, {
    key: 'writeResponseForRequest',
    value: function writeResponseForRequest(response, request) {
      if (!request.response_topic) {
        return _bluebird.Promise.reject('Request is missing a response_topic. Request: %j');
      } else {
        var kafkaMessageObject = this.createMessageResponseObject(response, request);
        return this.writeMessageToTopic(JSON.stringify(kafkaMessageObject), request.response_topic);
      }
    }

    /**
    * write the given message to the kafka topic
    * @param {string} messageString - message to write on topic
    * @param {string} topic - Kafka topic to write to
    * @param {object {attemptNumber: maxAttempts: withBackoff}} configs
          attemptNumber: int - current attempt number
          maxAttempts: int - maximum number of attempts
          withBackoff - boolean - whether or not to use exponential backoff
    * @returns {Promise<String>} response of Kafka to a successful write
    */
  }, {
    key: 'writeMessageToTopic',
    value: function writeMessageToTopic(messageString, topic) {
      var _this = this;

      var configs = arguments.length <= 2 || arguments[2] === undefined ? {} : arguments[2];

      var attemptNumber = configs.attemptNumber || 0;
      var maxAttempts = configs.maxAttempts || 10;
      var withBackoff = configs.withBackoff !== undefined ? configs.withBackoff : true;
      return this.initializeProducer().then(function (producer) {
        return new _bluebird.Promise(function (resolve, reject) {
          producer.send([{
            topic: topic,
            messages: [messageString]
          }], function (err, data) {
            if (err) {
              // retry if there was an error w/ exponential backoff. Wrap the retry in a promise
              if (attemptNumber < maxAttempts - 1) {
                if (withBackoff) {
                  setTimeout(function () {
                    return resolve(_this.writeMessageToTopic(messageString, topic, {
                      attemptNumber: attemptNumber + 1,
                      maxAttempts: maxAttempts,
                      withBackoff: withBackoff
                    }));
                  }, Math.pow(2, attemptNumber) * 1000 + Math.round(Math.random() * 1000));
                } else {
                  resolve(_this.writeMessageToTopic(messageString, topic, {
                    attemptNumber: attemptNumber + 1,
                    maxAttempts: maxAttempts,
                    withBackoff: withBackoff
                  }));
                }
              } else {
                reject(err);
              }
            } else {
              resolve(data);
            }
          });
        });
      });
    }

    /**
    * create Kafka topics
    */
  }, {
    key: 'createTopics',
    value: function createTopics() {
      for (var _len = arguments.length, topicNames = Array(_len), _key = 0; _key < _len; _key++) {
        topicNames[_key] = arguments[_key];
      }

      return this.initializeProducer().then(function (producer) {
        return new _bluebird.Promise(function (resolve, reject) {
          producer.createTopics(topicNames, false, function (err, data) {
            if (err) {
              reject(err);
            } else {
              resolve(data);
            }
          });
        });
      });
    }
  }], [{
    key: 'isMessageRequest',
    value: function isMessageRequest(message) {
      return _.contains(message, 'response_topic');
    }
  }]);

  return KafkaAdapter;
})();

exports.KafkaAdapter = KafkaAdapter;
;