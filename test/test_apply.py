from unittest import TestCase

from apply import apply as apply2

class TestApply(TestCase):

	def test_apply(self):

		tests = [
			{# fresh write
				'existing_data' : None,
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'sum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 3, 6]},
				'expected' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 3, 6]},
				'expected_stats' : [
					'apply.sum.count'
				]
			},
			{# sum write, 0s
				'existing_data' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 0, 0]},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'sum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 3, 6]},
				'expected' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [2, 3, 6]},
				'expected_stats' : [
					'apply.sum.count'
				]
			},
			{# sum write, existing None
				'existing_data' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, None, None]},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'sum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 3, 6]},
				'expected' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [2, 3, 6]},
				'expected_stats' : [
					'apply.sum.count'
				]
			},
			{# sum write, None
				'existing_data' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 3, 6]},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'sum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, None, None]},
				'expected' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [2, 3, 6]},
				'expected_stats' : [
					'apply.sum.count'
				]
			},
			{# average write, existing None
				'existing_data' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, None, 6]},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'average', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 0, 0]},
				'expected' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 0, 3]},
				'expected_stats' : [
					'apply.average.count'
				]
			},
			{# average write, None
				'existing_data' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 0, 6]},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'average', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, None, 0]},
				'expected' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 0, 3]},
				'expected_stats' : [
					'apply.average.count'
				]
			},
			{# minimum write, existing None
				'existing_data' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, None, 6]},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'minimum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 0, 0]},
				'expected' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 0, 0]},
				'expected_stats' : [
					'apply.minimum.count'
				]
			},
			{# minimum write, None
				'existing_data' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 0, 6]},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'minimum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, None, 0]},
				'expected' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 0, 0]},
				'expected_stats' : [
					'apply.minimum.count'
				]
			},
			{# maximum write, existing None
				'existing_data' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, None, 6]},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'maximum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 0, 0]},
				'expected' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 0, 6]},
				'expected_stats' : [
					'apply.maximum.count'
				]
			},
			{# maximum write, None
				'existing_data' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 0, 6]},
				'change' : {'metric' : 'users.registered.count', 'aggregation_type' : 'maximum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, None, 0]},
				'expected' : {'metric' : 'users.registered.count', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1, 0, 6]},
				'expected_stats' : [
					'apply.maximum.count'
				]
			}
		]

		test_counter = 0
		for test in tests:
			test_counter += 1

			# tear down mock dynamodb
			# set up mock dynamodb
			# populate mock dynamodb with test['existing_data']

			out = apply2(test['change'])
			self.assertListEqual(out, test['expected'], '[%d] Test expected %s, got %s' % (test_counter, test['expected'], out))
