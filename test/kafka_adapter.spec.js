import { KafkaAdapter } from '../src';
import * as chai from 'chai';
import * as sinon from 'sinon';
import * as bluebird from 'bluebird';
import * as _ from 'lodash';

let kafkaMessageResponse = 'Message successfully delivered';

/**
 * canSend: whether or not this producer can send messages without errors
 */
var createProducer = function(canSend) {
  return {
    send: (...args) => {
      var callback = _.last(args);
      if (! canSend) { 
        callback('Cannot send to kafka.', null);
      } else {
        callback(null, kafkaMessageResponse);
      }
    }
  }
}

describe('kafka adapter', () => {
  var adapter;
  beforeEach(() => {
    adapter = new KafkaAdapter('fake_zookeeper_address');
  });
  describe('createMessageResponseObject', () => {
    it('handles malformed requests that are missing correlation_id', function () {
      let malformedRequestMissingCorrelationId = {
        body: 'query TestQuery {}',
        request_id: 'request_id',
        request_topic: 'example_request_topic'
      };
      let response = {data: {}};
      let kafkaMessageObject = adapter.createMessageResponseObject(
        response,
        malformedRequestMissingCorrelationId
      );
      chai.expect(kafkaMessageObject).to.have.property('errors').with.length(1);
    });
    it('handles malformed requests that are missing request_id', function () {
      let malformedRequestMissingCorrelationId = {
        body: 'query TestQuery {}',
        correlation_id: 'correlation_id',
        request_topic: 'example_request_topic'
      };
      let response = {data: {}};
      let kafkaMessageObject = adapter.createMessageResponseObject(
        response,
        malformedRequestMissingCorrelationId
      );
      chai.expect(kafkaMessageObject).to.have.property('errors').with.length(1);
    });
    it('creates an object w/ a response and request id and correlation id', () => {
      let correlationId = 'test_correlation_id';
      let requestId = 'test_request_id';
      let validRequest = {
        body: 'query TestQuery {}',
        request_id: requestId,
        correlation_id: correlationId
      };
      let response = {data: {}};
      let kafkaMessageObject = adapter.createMessageResponseObject(
        response,
        validRequest
      );
      chai.expect(kafkaMessageObject).to.deep.equal({
        response: response,
        request_id: requestId,
        correlation_id: correlationId
      });
    });
  });
  
  describe('writeResponseForRequest', () => {
    it('errors on malformed requests that lack request_topic', function (done) {
      let malformedRequest = {
        body: 'query TestQuery{}',
        request_id: 'test_id',
        correlation_id: 'test_id'
      };
      adapter.writeResponseForRequest({}, malformedRequest)
      .catch((err) => {
        // check if we throw the error
        done();
      });
    });
    it('calls writeMessageToTopic with a valid kafka message object on valid requests', sinon.test(function (done) {
      let validRequest = {
        body: 'query testQuery{}',
        correlation_id: 'test_id',
        request_id: 'test_id',
        response_topic: 'test_topic'
      };
      let graphqlResponse = {data: {}};
      let responseKafkaMessage = {
        response: graphqlResponse,
        correlation_id: validRequest.correlation_id,
        request_id: validRequest.request_id
      };
      let kafkaSendResponse = 'kafka message sent';

      let writeMessageToTopicStub = this.stub(adapter, 'writeMessageToTopic');
      writeMessageToTopicStub.withArgs(JSON.stringify(responseKafkaMessage), validRequest.response_topic).returns(
        new bluebird.Promise((resolve, reject) => (resolve(kafkaSendResponse)))
      );

      adapter.writeResponseForRequest(graphqlResponse, validRequest)
      .then((message) => {
        chai.expect(message).to.equal(kafkaSendResponse);
				done();
      })
      .catch(err => {
        console.error(err);
      });
    }));
  });
  
  describe('writeKafkaMessageToTopic', function () {
    it('retries if writing fails', sinon.test(function (done) {
      this.stub(adapter, 'initializeProducer').returns(new bluebird.Promise((resolve, reject) => {
        resolve(createProducer(false));
      }));
      var writeMessageToTopicSpy = this.spy(adapter, 'writeMessageToTopic');
      adapter.writeMessageToTopic('my message', 'test_topic', {
        attemptNumber: 0,
        maxAttempts: 10,
        withBackoff: false
      })
      .catch(() => {
        chai.expect(writeMessageToTopicSpy.callCount).to.equal(10);
        done();
      })
    }));
    it('returns the response from Kafka on sending the message', sinon.test(function (done) {
      this.stub(adapter, 'initializeProducer').returns(new bluebird.Promise((resolve, reject) => {
        resolve(createProducer(true));
      }));
      adapter.writeMessageToTopic('my message', 'test_topic', {
        attemptNumber: 0,
        maxAttempts: 10,
        withBackoff: false
      })
      .then((message) => {
        chai.expect(message).to.equal(kafkaMessageResponse);
        done();
      })
    }));
  });
});
