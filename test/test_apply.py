from unittest import TestCase

from apply import apply as apply2, get_table_name, TABLE_PREFIX, TABLE_JOINER
from schema import DynamoDB_Schema
from message import Message as TMessage

from boto.dynamodb2.table import Table
from boto.dynamodb2.layer1 import DynamoDBConnection

from boto.sqs.regioninfo import SQSRegionInfo
from boto.sqs.message import Message

from boto.exception import JSONResponseError

from statsd import StatsClient
from . import assertStatsd

class TestApply(TestCase):

	twenty_sec_table_name = get_table_name('20sec', '01-04-2014')

	def setUp(self):
		
		self.dynamodb_connection = DynamoDBConnection(
			host='localhost',
			port=8000,
			aws_access_key_id='id',
			aws_secret_access_key='secret',
			is_secure=False
		)

		client = StatsClient(host='localhost', port=8125, prefix=None, maxudpsize=512)

		self.statsd = client.pipeline()

		sqsregioninfo = SQSRegionInfo(name='localhost_region', endpoint='localhost')
		self.sqs_connection = sqsregioninfo.connect(
			port=8001,
			aws_access_key_id='id',
			aws_secret_access_key='secret',
			is_secure=False
		)

		self.indexer_queue = self.sqs_connection.create_queue('test_indexer_queue')

	def tear_down_table(self):

		try:
			Table(self.twenty_sec_table_name, connection=self.dynamodb_connection).delete()
		except JSONResponseError, e:

			if e.error_code == 'ResourceNotFoundException':
				pass

			else:
				raise e

	def tearDown(self):

		self.sqs_connection.delete_queue(self.indexer_queue)
		self.tear_down_table()

	def test_apply(self):

		tests = [
			{# fresh write, table doesn't exist
				'change' : TMessage(metric = 'users.registered', aggregation_type = 'sum', start_time = '01-04-2014 14:35:00', resolution = '20sec', datapoints = [1, 3, 6]),
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 3, 6]'}},
				'expected_stats' : [
					'apply.dynamodb.table.describe',
					'apply.dynamodb.table.create',
					'apply.dynamodb.table.get_item',
					'apply.metric.create',
					'apply.dynamodb.item.write',
					'apply.sqs.indexer.write'
				],
				'expected_indexer_message' : {'body' : 'users.registered', 'resolution' : '20sec'}
			},
			{# fresh write, table exists
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : []
				},
				'change' : TMessage(metric = 'users.registered', aggregation_type = 'sum', start_time = '01-04-2014 14:35:00', resolution = '20sec', datapoints = [1, 3, 6]),
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 3, 6]'}},
				'expected_stats' : [
					'apply.dynamodb.table.describe',
					'apply.dynamodb.table.get_item',
					'apply.metric.create',
					'apply.dynamodb.item.write',
					'apply.sqs.indexer.write'
				]
			},
			{# sum write, 0s
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 0]'}
					]
				},
				'change' : TMessage(metric = 'users.registered', aggregation_type = 'sum', start_time = '01-04-2014 14:35:00', resolution = '20sec', datapoints = [1, 3, 6]),
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[2, 3, 6]'}},
				'expected_stats' : [
					'apply.dynamodb.table.describe',
					'apply.dynamodb.table.get_item',
					'apply.dynamodb.item.write',
					'apply.metric.update',
					'apply.aggregate.sum'
				]
			},
			{# sum write, existing None
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, null, null]'}
					]
				},
				'change' : TMessage(metric = 'users.registered', aggregation_type = 'sum', start_time = '01-04-2014 14:35:00', resolution = '20sec', datapoints = [1, 3, 6]),
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[2, 3, 6]'}},
				'expected_stats' : [
					'apply.dynamodb.table.describe',
					'apply.dynamodb.table.get_item',
					'apply.dynamodb.item.write',
					'apply.metric.update',
					'apply.aggregate.sum'
				]
			},
			{# sum write, None
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 3, 6]'}
					]
				},
				'change' : TMessage(metric = 'users.registered', aggregation_type = 'sum', start_time = '01-04-2014 14:35:00', resolution = '20sec', datapoints = [1, None, None]),
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[2, 3, 6]'}},
				'expected_stats' : [
					'apply.dynamodb.table.describe',
					'apply.dynamodb.table.get_item',
					'apply.dynamodb.item.write',
					'apply.metric.update',
					'apply.aggregate.sum'
				]
			},
			{# average write, existing None
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, null, 6]'}
					]
				},
				'change' : TMessage(metric = 'users.registered', aggregation_type = 'average', start_time = '01-04-2014 14:35:00', resolution = '20sec', datapoints = [1, 0, 0]),
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 3]'}},
				'expected_stats' : [
					'apply.dynamodb.table.describe',
					'apply.dynamodb.table.get_item',
					'apply.dynamodb.item.write',
					'apply.metric.update',
					'apply.aggregate.average'
				]
			},
			{# average write, None
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 6]'}
					]
				},
				'change' : TMessage(metric = 'users.registered', aggregation_type = 'average', start_time = '01-04-2014 14:35:00', resolution = '20sec', datapoints = [1, None, 0]),
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 3]'}},
				'expected_stats' : [
					'apply.dynamodb.table.describe',
					'apply.dynamodb.table.get_item',
					'apply.dynamodb.item.write',
					'apply.metric.update',
					'apply.aggregate.average'
				]
			},
			{# minimum write, existing None
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, null, 6]'}
					]
				},
				'change' : TMessage(metric = 'users.registered', aggregation_type = 'minimum', start_time = '01-04-2014 14:35:00', resolution = '20sec', datapoints = [1, 0, 0]),
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 0]'}},
				'expected_stats' : [
					'apply.dynamodb.table.describe',
					'apply.dynamodb.table.get_item',
					'apply.dynamodb.item.write',
					'apply.metric.update',
					'apply.aggregate.minimum'
				]
			},
			{# minimum write, None
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 6]'}
					]
				},
				'change' : TMessage(metric = 'users.registered', aggregation_type = 'minimum', start_time = '01-04-2014 14:35:00', resolution = '20sec', datapoints = [1, None, 0]),
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 0]'}},
				'expected_stats' : [
					'apply.dynamodb.table.describe',
					'apply.dynamodb.table.get_item',
					'apply.dynamodb.item.write',
					'apply.metric.update',
					'apply.aggregate.minimum'
				]
			},
			{# maximum write, existing None
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, null, 6]'}
					]
				},
				'change' : TMessage(metric = 'users.registered', aggregation_type = 'maximum', start_time = '01-04-2014 14:35:00', resolution = '20sec', datapoints = [1, 0, 0]),
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 6]'}},
				'expected_stats' : [
					'apply.dynamodb.table.describe',
					'apply.dynamodb.table.get_item',
					'apply.dynamodb.item.write',
					'apply.metric.update',
					'apply.aggregate.maximum'
				]
			},
			{# maximum write, None
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 6]'}
					]
				},
				'change' : TMessage(metric = 'users.registered', aggregation_type = 'maximum', start_time = '01-04-2014 14:35:00', resolution = '20sec', datapoints = [1, None, 0]),
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 6]'}},
				'expected_stats' : [
					'apply.dynamodb.table.describe',
					'apply.dynamodb.table.get_item',
					'apply.dynamodb.item.write',
					'apply.metric.update',
					'apply.aggregate.maximum'
				]
			}
		]

		test_counter = 0
		for test in tests:
			test_counter += 1
			
			if test.has_key('existing_data'):
				populate_tables(self.dynamodb_connection, test['existing_data'])

			self.statsd._stats = []

			apply2(
				message=test['change'],
				dynamodb_connection=self.dynamodb_connection,
				indexer_queue=self.indexer_queue,
				statsd=self.statsd
			)
			
			e = test['expected']['item']

			out = get_metric(test['expected']['table_name'], self.dynamodb_connection, e['metric'], e['start_time'])

			self.assertDictEqual(out, test['expected']['item'], '[%d] Test expected %s, got %s' % (test_counter, test['expected']['item'], out))
			assertStatsd(self, self.statsd, test['expected_stats'], test_counter, '[%d] Test expected %s, got %s')
			
			if test.has_key('expected_indexer_message'):

				indexer_messages = self.indexer_queue.get_messages(message_attributes=['*'])
				self.assertEquals(len(indexer_messages), 1, '[%d] Test expected an indexer message, got none.' % test_counter)

				indexer_message = {'body' : indexer_messages[0].get_body(), 'resolution' : indexer_messages[0].message_attributes['resolution']['string_value']}
				self.assertDictEqual(indexer_message, test['expected_indexer_message'], '[%d] Test expected %s, got %s' % (test_counter, test['expected_indexer_message'], indexer_message))

			self.indexer_queue.clear()
			self.tear_down_table()

	def test_get_table_name(self):

		tests = [
			{# 20 sec
				'resolution' : '20sec',
				'start_time' : '01-04-2014 14:35:00',
				'expected' : TABLE_PREFIX + TABLE_JOINER + '01-04-2014' + TABLE_JOINER + '20sec'
			},
			{# 2 min
				'resolution' : '2min',
				'start_time' : '01-12-2013 14:35:00',
				'expected' : TABLE_PREFIX + TABLE_JOINER + '01-12-2013' + TABLE_JOINER + '2min'
			},
			{# Invalid start time
				'resolution' : '20min',
				'start_time' : '2014-01-04 14:35:00',
				'expected' : Exception('Invalid start time.')
			}
		]

		test_counter = 0
		for test in tests:
			test_counter += 1
			
			try:
				out = get_table_name(test['resolution'], test['start_time'])
			except Exception, e:
				out = e.args
				test["expected"] = test["expected"].args

			self.assertEquals(out, test['expected'], '[%d] Test expected %s, got %s' % (test_counter, test['expected'], out))

def populate_tables(connection, table_data):

	try:
		table = Table(table_data['table_name'], connection=connection)
		table.describe()

	except JSONResponseError, e:

		if e.error_code == 'ResourceNotFoundException':
			table = Table.create(
				table_name=table_data['table_name'],
				schema=DynamoDB_Schema,
				connection=connection
			)
		else:
			raise e

	with table.batch_write() as batch:
		for item in table_data['items']:
			batch.put_item(data=item)

def get_metric(table_name, connection, metric_name, start_time):

	table = Table(table_name=table_name, connection=connection)

	item = table.get_item(metric=metric_name, start_time=start_time)

	return dict(item)
