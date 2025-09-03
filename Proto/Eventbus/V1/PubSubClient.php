<?php
// GENERATED CODE -- DO NOT EDIT!

// Original file comments:
//
// Salesforce Pub/Sub API Version 1.
//
namespace Eventbus\V1;

/**
 *
 * The Pub/Sub API provides a single interface for publishing and subscribing to platform events, including real-time
 * event monitoring events, and change data capture events. The Pub/Sub API is a gRPC API that is based on HTTP/2.
 *
 * A session token is needed to authenticate. Any of the Salesforce supported
 * OAuth flows can be used to obtain a session token:
 * https://help.salesforce.com/articleView?id=sf.remoteaccess_oauth_flows.htm&type=5
 *
 * For each RPC, a client needs to pass authentication information
 * as metadata headers (https://www.grpc.io/docs/guides/concepts/#metadata) with their method call.
 *
 * For Salesforce session token authentication, use:
 *   accesstoken : access token
 *   instanceurl : Salesforce instance URL
 *   tenantid : tenant/org id of the client
 *
 * StatusException is thrown in case of response failure for any request.
 */
class PubSubClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     *
     * Bidirectional streaming RPC to subscribe to a Topic. The subscription is pull-based. A client can request
     * for more events as it consumes events. This enables a client to handle flow control based on the client's processing speed.
     *
     * Typical flow:
     * 1. Client requests for X number of events via FetchRequest.
     * 2. Server receives request and delivers events until X events are delivered to the client via one or more FetchResponse messages.
     * 3. Client consumes the FetchResponse messages as they come.
     * 4. Client issues new FetchRequest for Y more number of events. This request can
     *    come before the server has delivered the earlier requested X number of events
     *    so the client gets a continuous stream of events if any.
     *
     * If a client requests more events before the server finishes the last
     * requested amount, the server appends the new amount to the current amount of
     * events it still needs to fetch and deliver.
     *
     * A client can subscribe at any point in the stream by providing a replay option in the first FetchRequest.
     * The replay option is honored for the first FetchRequest received from a client. Any subsequent FetchRequests with a
     * new replay option are ignored. A client needs to call the Subscribe RPC again to restart the subscription
     * at a new point in the stream.
     *
     * The first FetchRequest of the stream identifies the topic to subscribe to.
     * If any subsequent FetchRequest provides topic_name, it must match what
     * was provided in the first FetchRequest; otherwise, the RPC returns an error
     * with INVALID_ARGUMENT status.
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\BidiStreamingCall
     */
    public function Subscribe($metadata = [], $options = []) {
        return $this->_bidiRequest('/eventbus.v1.PubSub/Subscribe',
        ['\Eventbus\V1\FetchResponse','decode'],
        $metadata, $options);
    }

    /**
     * Get the event schema for a topic based on a schema ID.
     * @param \Eventbus\V1\SchemaRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Eventbus\V1\SchemaInfo>
     */
    public function GetSchema(\Eventbus\V1\SchemaRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/eventbus.v1.PubSub/GetSchema',
        $argument,
        ['\Eventbus\V1\SchemaInfo', 'decode'],
        $metadata, $options);
    }

    /**
     *
     * Get the topic Information related to the specified topic.
     * @param \Eventbus\V1\TopicRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Eventbus\V1\TopicInfo>
     */
    public function GetTopic(\Eventbus\V1\TopicRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/eventbus.v1.PubSub/GetTopic',
        $argument,
        ['\Eventbus\V1\TopicInfo', 'decode'],
        $metadata, $options);
    }

    /**
     *
     * Send a publish request to synchronously publish events to a topic.
     * @param \Eventbus\V1\PublishRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Eventbus\V1\PublishResponse>
     */
    public function Publish(\Eventbus\V1\PublishRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/eventbus.v1.PubSub/Publish',
        $argument,
        ['\Eventbus\V1\PublishResponse', 'decode'],
        $metadata, $options);
    }

    /**
     *
     * Bidirectional Streaming RPC to publish events to the event bus.
     * PublishRequest contains the batch of events to publish.
     *
     * The first PublishRequest of the stream identifies the topic to publish on.
     * If any subsequent PublishRequest provides topic_name, it must match what
     * was provided in the first PublishRequest; otherwise, the RPC returns an error
     * with INVALID_ARGUMENT status.
     *
     * The server returns a PublishResponse for each PublishRequest when publish is
     * complete for the batch. A client does not have to wait for a PublishResponse
     * before sending a new PublishRequest, i.e. multiple publish batches can be queued
     * up, which allows for higher publish rate as a client can asynchronously
     * publish more events while publishes are still in flight on the server side.
     *
     * PublishResponse holds a PublishResult for each event published that indicates success
     * or failure of the publish. A client can then retry the publish as needed before sending
     * more PublishRequests for new events to publish.
     *
     * A client must send a valid publish request with one or more events every 70 seconds to hold on to the stream.
     * Otherwise, the server closes the stream and notifies the client. Once the client is notified of the stream closure,
     * it must make a new PublishStream call to resume publishing.
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\BidiStreamingCall
     */
    public function PublishStream($metadata = [], $options = []) {
        return $this->_bidiRequest('/eventbus.v1.PubSub/PublishStream',
        ['\Eventbus\V1\PublishResponse','decode'],
        $metadata, $options);
    }

    /**
     *
     * This feature is part of an open beta release and is subject to the applicable
     * Beta Services Terms provided at Agreements and Terms
     * (https://www.salesforce.com/company/legal/agreements/).
     *
     * Same as Subscribe, but for Managed Subscription clients.
     * This feature is part of an open beta release.
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\BidiStreamingCall
     */
    public function ManagedSubscribe($metadata = [], $options = []) {
        return $this->_bidiRequest('/eventbus.v1.PubSub/ManagedSubscribe',
        ['\Eventbus\V1\ManagedFetchResponse','decode'],
        $metadata, $options);
    }

}
