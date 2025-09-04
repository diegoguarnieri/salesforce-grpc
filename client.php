<?php

require_once __DIR__ . '/vendor/autoload.php';

use Auxmoney\Avro\AvroFactory;
use Dotenv\Dotenv;
use Eventbus\V1\FetchRequest;
use Eventbus\V1\FetchResponse;
use Eventbus\V1\PubSubClient;
use Eventbus\V1\ReplayPreset;
use Eventbus\V1\SchemaRequest;
use Eventbus\V1\TopicRequest;

$dotenv = Dotenv::createImmutable(__DIR__);
$dotenv->load();

$accessToken = getAccessToken();
$instance = $_ENV['SF_INSTANCE'];
$apiEndpoint = 'api.pubsub.salesforce.com';
$tenantId = $_ENV['SF_TENANT_ID'];

$client = new PubSubClient("$apiEndpoint:7443", [
    'credentials' => \Grpc\ChannelCredentials::createSsl(),
    'update_metadata' => function($metadata) use ($accessToken, $instance, $tenantId) {
        $metadata['accesstoken'] = [$accessToken];
        $metadata['instanceurl'] = [$instance];
        $metadata['tenantid'] = [$tenantId];
        return $metadata;
    }
]);

//$topic = '/data/TaskChangeEvent';
$topic = '/data/LeadChangeEvent';

// Request Avro schema for Lead CDC
$request = new TopicRequest();
$request->setTopicName($topic);

list($response, $status) = $client->GetTopic($request)->wait();

if($status->code !== \Grpc\STATUS_OK) {
    echo "Error: " . $status->details . PHP_EOL;
    exit(1);
}

// The schema comes as JSON Avro string
$schemaId = $response->getSchemaId();

$schema = new SchemaRequest();
$schema->setSchemaId($schemaId);
list($getSchemaResponse, $status) = $client->GetSchema($schema)->wait();

if($status->code !== Grpc\STATUS_OK) {
    throw new Exception("GetSchema failed with status: {$status->code} - {$status->details}");
}

$avroSchemaJson = $getSchemaResponse->getSchemaJson();
//echo "Retrieved Avro Schema:\n";
//echo $avroSchemaJson . "\n";

//subscribe to a topic
$call = $client->Subscribe();

$fetchRequest = new FetchRequest();
$fetchRequest->setTopicName($topic);
$fetchRequest->setNumRequested(10);
$fetchRequest->setReplayPreset(ReplayPreset::EARLIEST);

//initial request
$call->write($fetchRequest);

while($response = $call->read()) {

    if($response instanceof FetchResponse) {
        $events = $response->getEvents();

        foreach($events as $event) {
            $replayId = base64_encode($event->getReplayId());

            echo "-----------------------------------------------\n";
            echo "Received event with Replay ID: $replayId\n";
            $avroPayload = $event->getEvent()->getPayload();

            $avroFactory = AvroFactory::create();
            $reader = $avroFactory->createReader($avroSchemaJson);
            $buffer = $avroFactory->createReadableStreamFromString($avroPayload);
            $decodedData = $reader->read($buffer);

            echo "Entity: " . $decodedData['ChangeEventHeader']['entityName'] . "\n";
            echo "Change Type: " . $decodedData['ChangeEventHeader']['changeType'] . "\n";
            echo "Commit Timestamp: " . (DateTime::createFromFormat('U', (string) $decodedData['ChangeEventHeader']['commitTimestamp'] / 1000))->format('Y-m-d H:i:s T') . "\n";
            echo "Fields:\n";
            foreach($decodedData as $key => $value) {
                if($key === 'ChangeEventHeader') {
                    echo "   Ids: " . json_encode($value['recordIds'] ?? []) . "\n";
                } else {
                    if(is_array($value)) {
                        foreach($value as $subKey => $subValue) {
                            printAAA($subKey, $subValue);
                        }
                    } else {
                        printAAA($key, $value);
                    }
                }
            }
        }

        // Send a new FetchRequest to request more events
        $call->write($fetchRequest);
    }
}

$call->cancel();


function printAAA($key, $value) {
    if(!is_null($value)) echo "   " . str_pad($key, 30, " ", STR_PAD_RIGHT) . ": " . $value . "\n";
}

function getAccessToken() {
    $url = 'https://login.salesforce.com/services/oauth2/token';

    $params = [
        'grant_type' => 'refresh_token',
        'client_id' => $_ENV['SF_CLIENT_ID'],
        'client_secret' => $_ENV['SF_CLIENT_SECRET'],
        'refresh_token' => $_ENV['SF_REFRESH_TOKEN'],
    ];

    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_POST, true);
    curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($params));
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

    $response = curl_exec($ch);

    var_dump($response);

    curl_close($ch);

    return json_decode($response, true)['access_token'] ?? null;
}
