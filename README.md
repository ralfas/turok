# turok

## Introduction

Turok is a timeseries database abstraction over DynamoDB.

## Architecture Overview

Turok consists of:
- A library - `turok-lib` - for transforming metric data into Turok compatible data structures. These can then be pushed onto SQS.
- An aggregator application - `turokd` - that consumes from SQS, applies aggregation rules as defined in the message and writes to DynamoDB.
- A Graphite finder - `graphite-turok-reader` - that is able to read data from DynamoDB and present it to Graphite.
- A Data Pipeline template - `turok-keeper` - that applies a configured retention policy per data resolution.

## Turok-Lib

TODO

### Turok Message Format

```
Message:
{
	metric : String; `node1.messages_received`
	start_time : String; `01-04-2014 14:35:00`
	datapoints : JSON Encoded Datapoint List; [1, 2, 30000, 3, 40]
	resolution : String; `20sec`, `1min`
	aggregation_type : String; `sum`, `average`, `minimum`, `maximum`
}
```

## TurokD

TODO

### Turok DynamoDB Schema

```
Table name: <TABLE_PREFIX><TABLE_JOINER><DATE><TABLE_JOINER><RESOLUTION> ; `turok_01-04-2014_20sec`
{
	metric : String; `node1.messages_received`
	start_time : String; `01-04-2014 14:35:00`
	datapoints : JSON Encoded Datapoint List; `[1, 2, 30000, 3, 40]`
}
```

Tables are partitioned by date:
- to make applying retention policies simpler,
- to make read and write DynamoDB provisioning optimisations possible.

DynamoDB only supports sets and not lists, so we need to serialise it for persistence.

## Turok-Reader

TODO

Reference: http://graphite.readthedocs.org/en/latest/storage-backends.html

## Turok-Keeper

TODO

Reference: http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/what-is-datapipeline.html

## Development Environment

### Install Dependencies

#### Install python libraries

Install `boto` and `moto`:

	pip install boto moto

#### Install DynamoDB Local

Download the latest release of DynamoDB Local:

	http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest

Un-tar, rename and move to the `bin` directory:

	tar -xf dynamodb_local_2014-04-24.tar.gz
	mv dynamodb_local_2014-04-24 turok/bin/dynamodb_local

Reference: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html

### Run Tests

Start DynamoDB Local:

	java -Djava.library.path=./bin/dynamodb_local/DynamoDBLocal_lib -jar bin/dynamodb_local/DynamoDBLocal.jar --inMemory

Start SQS moto_server:

	moto_server -p 8001 sqs

Run tests:

	./bin/run_tests.sh
