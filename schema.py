from boto.dynamodb2.fields import HashKey, RangeKey

SCHEMA = [
	HashKey('metric'),
	RangeKey('start_time')
]