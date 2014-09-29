#!/usr/bin/env python

from fetch import fetch, MAX_ITEMS
from apply import apply as apply2
from statsd import StatsClient

from boto.sqs.regioninfo import SQSRegionInfo
from boto.dynamodb2.layer1 import DynamoDBConnection

def get_dynamoDBconnection():
	
	connection = DynamoDBConnection(
		host='localhost',
		port=8000,
		aws_access_key_id='id',
		aws_secret_access_key='secret',
		is_secure=False
	)

	return connection

def get_SQSqueue():
	
	sqsregioninfo = SQSRegionInfo(name='localhost_region', endpoint='localhost')
	connection = sqsregioninfo.connect(
		port=8001,
		aws_access_key_id='id',
		aws_secret_access_key='secret',
		is_secure=False
	)

	return connection.get_queue('test_queue')

def main():

	statsd = StatsClient(host='localhost', port=8125, prefix=None, maxudpsize=512)

	queue = get_SQSqueue()
	db_connection = get_dynamoDBconnection()

	while True:
		messages = fetch(items=MAX_ITEMS, queue=queue, statsd=statsd)

		for message in messages:
			if apply2(message=message, connection=db_connection, statsd=statsd):
				pass
				# delete message from queue

if __name__ == '__main__':
	main()