#! /bin/bash
PATH_GOGOPROTOBUF=$GOPATH/src/github.com/gogo/protobuf
protoc --proto_path=$GOPATH/src/:$PATH_GOGOPROTOBUF/protobuf/:. --gogofaster_out=. *.proto
