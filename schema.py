from boto.dynamodb2.fields import HashKey, RangeKey, BaseSchemaField

SCHEMA = [
	HashKey('metric'),
	RangeKey('start_time')
]