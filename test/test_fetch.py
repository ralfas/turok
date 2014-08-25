from unittest import TestCase

from fetch import fetch, MAX_ITEMS

class SQSDummyQueue(object):

	def __init__(self, messages=[]):

		self.messages = []
		for message in messages:
			self.messages.append(SQSDummyMessage(message))

	def get_messages(self, num_messages=1, visibility=30):

		out = self.messages[:num_messages]
		self.messages = self.messages[num_messages:]

		return out

class SQSDummyMessage(object):
	
	def __init__(self, body):
		self.body = body

	def get_body(self):
		return self.body

class TestFetch(TestCase):

	def test_fetch(self):

		tests = [
			{# no changes
				'changes' : [],
				'expected' : [],
				'expected_stats' : [
					'fetch.empty.count'
				]
			},
			{# one change
				'changes' : [
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}'
				],
				'expected' : [
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}'
				],
				'expected_stats' : [
					'fetch.successful.count',
					'fetch.items.count'
				]
			},
			{# 10 changes, boundary test
				'changes' : [
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}'
				],
				'expected' : [
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}'
				],
				'expected_stats' : [
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count'
				]
			},
			{# 11 changes, boundary test
				'changes' : [
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}'
				],
				'expected' : [
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}'
				],
				'expected_stats' : [
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count',
					'fetch.successful.count',
					'fetch.items.count'
				]
			},
			{# arbitrary data
				'changes' : [
					"arbitrary data"
				],
				'expected' : [
					"arbitrary data"
				],
				'expected_stats' : [
					'fetch.successful.count',
					'fetch.items.count'
				]
			}
		]

		test_counter = 0
		for test in tests:
			test_counter += 1

			# tear down mock SQS
			# set up mock SQS
			# populate mock SQS with messages
			queue = SQSDummyQueue(test['changes'])
			
			out = fetch(items=MAX_ITEMS, queue=queue)
			out_messages = [o.get_body() for o in out]

			self.assertListEqual(out_messages, test['expected'], '[%d] Test expected %s, got %s' % (test_counter, test['expected'], out_messages))
