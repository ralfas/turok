from unittest import TestCase

from apply import apply as apply2, get_table_name, TABLE_PREFIX, TABLE_JOINER

from schema import DynamoDB_Schema

from boto.dynamodb2.table import Table
from boto.dynamodb2.layer1 import DynamoDBConnection

from boto.exception import JSONResponseError

class TestApply(TestCase):

	twenty_sec_table_name = get_table_name('20sec', '01-04-2014')

	def setUp(self):
		
		self.connection = DynamoDBConnection(
			host='localhost',
			port=8000,
			aws_access_key_id='id',
			aws_secret_access_key='secret',
			is_secure=False
		)

	def tearDown(self):

		try:
			Table(self.twenty_sec_table_name, connection=self.connection).delete()
		except JSONResponseError, e:

			if e.error_code == 'ResourceNotFoundException':
				pass

			else:
				raise e

	def test_apply(self):

		tests = [
			{# fresh write, table doesn't exist
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'sum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 3, 6]},
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 3, 6]'}},
				'expected_stats' : [
					'apply.sum.count'
				]
			},
			{# fresh write, table exists
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : []
				},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'sum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 3, 6]},
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 3, 6]'}},
				'expected_stats' : [
					'apply.sum.count'
				]
			},
			{# sum write, 0s
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 0]'}
					]
				},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'sum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 3, 6]},
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[2, 3, 6]'}},
				'expected_stats' : [
					'apply.sum.count'
				]
			},
			{# sum write, existing None
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, null, null]'}
					]
				},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'sum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 3, 6]},
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[2, 3, 6]'}},
				'expected_stats' : [
					'apply.sum.count'
				]
			},
			{# sum write, None
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 3, 6]'}
					]
				},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'sum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, None, None]},
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[2, 3, 6]'}},
				'expected_stats' : [
					'apply.sum.count'
				]
			},
			{# average write, existing None
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, null, 6]'}
					]
				},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'average', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 0, 0]},
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 3]'}},
				'expected_stats' : [
					'apply.average.count'
				]
			},
			{# average write, None
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 6]'}
					]
				},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'average', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, None, 0]},
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 3]'}},
				'expected_stats' : [
					'apply.average.count'
				]
			},
			{# minimum write, existing None
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, null, 6]'}
					]
				},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'minimum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 0, 0]},
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 0]'}},
				'expected_stats' : [
					'apply.minimum.count'
				]
			},
			{# minimum write, None
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 6]'}
					]
				},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'minimum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, None, 0]},
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 0]'}},
				'expected_stats' : [
					'apply.minimum.count'
				]
			},
			{# maximum write, existing None
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, null, 6]'}
					]
				},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'maximum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 0, 0]},
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 6]'}},
				'expected_stats' : [
					'apply.maximum.count'
				]
			},
			{# maximum write, None
				'existing_data' : {
					'table_name' : self.twenty_sec_table_name,
					'items' : [
						{'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 6]'}
					]
				},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'maximum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, None, 0]},
				'expected' : {'table_name' : self.twenty_sec_table_name, 'item' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'datapoints' : '[1, 0, 6]'}},
				'expected_stats' : [
					'apply.maximum.count'
				]
			}
		]

		test_counter = 0
		for test in tests:
			test_counter += 1
			
			if test.has_key('existing_data'):
				populate_tables(self.connection, test['existing_data'])

			c = test['change']

			apply2(
				metric=c['metric'],
				start_time=c['start_time'],
				resolution=c['resolution'],
				datapoints=c['datapoints'],
				aggregation_type=c['aggregation_type'],
				connection=self.connection
			)
			
			e = test['expected']['item']

			out = get_metric(test['expected']['table_name'], self.connection, e['metric'], e['start_time'])

			self.assertDictEqual(out, test['expected']['item'], '[%d] Test expected %s, got %s' % (test_counter, test['expected']['item'], out))

			self.tearDown()

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
