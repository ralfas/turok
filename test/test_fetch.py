from unittest import TestCase

from fetch import fetch, MAX_ITEMS
from message import Message as TMessage

from boto.sqs.regioninfo import SQSRegionInfo
from boto.sqs.message import Message

class TestFetch(TestCase):

	def setUp(self):

		sqsregioninfo = SQSRegionInfo(name='localhost_region', endpoint='localhost')
		self.connection = sqsregioninfo.connect(
			port=8001,
			aws_access_key_id='id',
			aws_secret_access_key='secret',
			is_secure=False
		)

		self.queue = self.connection.create_queue('test_queue')
	
	def populate_queue(self, messages):

		for item in messages:
			message = Message()
			message.set_body(item)
			self.queue.write(message)

	def tearDown(self):

		self.connection.delete_queue(self.queue)

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
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0])
				],
				'expected_stats' : [
					'fetch.successful.count',
					'fetch.items.count'
				]
			},
			{# Invalid change
				'changes' : [
					'{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : null}'
				],
				'expected' : [],
				'expected_stats' : [
					'fetch.invalid.values.count'
				]
			},
			{# Invalid JSON
				'changes' : [
					"{'metric' : 'users.registered.count', 'aggregation_type' : 'sum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1]}"
				],
				'expected' : [],
				'expected_stats' : [
					'fetch.invalid.json.count'
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
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0])
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
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0]),
					TMessage(metric = "users.registered.count", aggregation_type = "sum", start_time = "01-04-2014 14:35:00", resolution = "20sec", datapoints = [1.0, 2.0, 3.0])
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
			}
		]

		test_counter = 0
		for test in tests:
			test_counter += 1

			self.populate_queue(test['changes'])
			
			out = fetch(items=MAX_ITEMS, queue=self.queue)

			self.assertListEqual(out, test['expected'], '[%d] Test expected %s, got %s' % (test_counter, test['expected'], out))

			self.queue.clear()