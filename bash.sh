#!/bin/bash

protoc --proto_path /pool/apps/salesforce-grpc \
    --php_out=/pool/apps/salesforce-grpc/Proto/ \
    --grpc_out=/pool/apps/salesforce-grpc/Proto/ \
    --plugin=protoc-gen-grpc=/usr/local/bin/grpc_php_plugin \
    /pool/apps/salesforce-grpc/proto_files/pubsub_api.proto 
