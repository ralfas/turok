# turok

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

Run Tests:

	./bin/run_tests.sh
