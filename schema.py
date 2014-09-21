from boto.dynamodb2.fields import HashKey, RangeKey

DynamoDB_Schema = [
	HashKey('metric'),
	RangeKey('start_time')
]