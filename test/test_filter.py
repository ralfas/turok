from unittest import TestCase

from filter import filter as filter2

class TestFilter(TestCase):

	def test_filter(self):

		tests = [
			{# no changes
				"change" : None,
				"expected" : None,
				"expected_stats" : []
			},
			{# bad JSON
				"change" : "{'metric' : 'users.registered.count', 'aggregation_type' : 'sum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1.0, 2.0, 3.0]}",
				"expected" : None,
				"expected_stats" : [
					"filter.items.invalid.count"
				]
			},
			{# one
				"change" : '{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
				"expected" : {"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]},
				"expected_stats" : [
					"filter.items.valid.count"
				]
			},
			{# missing metric
				"change" : '{"metric" : "", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
				"expected" : None,
				"expected_stats" : [
					"filter.items.invalid.count"
				]
			},
			{# missing aggregation_type
				"change" : '{"metric" : "users.registered.count", "aggregation_type" : "", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
				"expected" : None,
				"expected_stats" : [
					"filter.items.invalid.count"
				]
			},
			{# invalid aggregation_type
				"change" : '{"metric" : "users.registered.count", "aggregation_type" : "minimumx", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
				"expected" : None,
				"expected_stats" : [
					"filter.items.invalid.count"
				]
			},
			{# valid aggregation_type sum
				"change" : '{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
				"expected" : {"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]},
				"expected_stats" : [
					"filter.items.valid.count"
				]
			},
			{# valid aggregation_type average
				"change" : '{"metric" : "users.registered.average", "aggregation_type" : "average", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
				"expected" : {"metric" : "users.registered.average", "aggregation_type" : "average", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]},
				"expected_stats" : [
					"filter.items.valid.count"
				]
			},
			{# valid aggregation_type minimum
				"change" : '{"metric" : "users.registered.minimum", "aggregation_type" : "minimum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
				"expected" : {"metric" : "users.registered.minimum", "aggregation_type" : "minimum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]},
				"expected_stats" : [
					"filter.items.valid.count"
				]
			},
			{# valid aggregation_type maximum
				"change" : '{"metric" : "users.registered.maximum", "aggregation_type" : "maximum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
				"expected" : {"metric" : "users.registered.maximum", "aggregation_type" : "maximum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]},
				"expected_stats" : [
					"filter.items.valid.count"
				]
			},
			{# missing start_time
				"change" : '{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
				"expected" : None,
				"expected_stats" : [
					"filter.items.invalid.count"
				]
			},
			{# invalid start_time
				"change" : '{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "2014-01-04 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
				"expected" : None,
				"expected_stats" : [
					"filter.items.invalid.count"
				]
			},
			{# missing resolution
				"change" : '{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "", "datapoints" : [1.0, 2.0, 3.0]}',
				"expected" : None,
				"expected_stats" : [
					"filter.items.invalid.count"
				]
			},
			{# invalid resolution, missing unit
				"change" : '{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "5", "datapoints" : [1.0, 2.0, 3.0]}',
				"expected" : None,
				"expected_stats" : [
					"filter.items.invalid.count"
				]
			},
			{# invalid resolution, missing number
				"change" : '{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "sec", "datapoints" : [1.0, 2.0, 3.0]}',
				"expected" : None,
				"expected_stats" : [
					"filter.items.invalid.count"
				]
			},
			{# valid resolution, sec
				"change" : '{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
				"expected" : {"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]},
				"expected_stats" : [
					"filter.items.valid.count"
				]
			},
			{# valid resolution, min
				"change" : '{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "5min", "datapoints" : [1.0, 2.0, 3.0]}',
				"expected" : {"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "5min", "datapoints" : [1.0, 2.0, 3.0]},
				"expected_stats" : [
					"filter.items.valid.count"
				]
			},
			{# invalid resolution, mins
				"change" : '{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "5mins", "datapoints" : [1.0, 2.0, 3.0]}',
				"expected" : None,
				"expected_stats" : [
					"filter.items.invalid.count"
				]
			},
			{# missing datapoints
				"change" : '{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : null}',
				"expected" : None,
				"expected_stats" : [
					"filter.items.invalid.count"
				]
			},
			{# null datapoints, no-op
				"change" : '{"metric" : "users.registered.count", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : []}',
				"expected" : None,
				"expected_stats" : [
					"filter.items.valid.count",
					"filter.items.no_op.count"
				]
			}
		]

		test_counter = 0
		for test in tests:
			test_counter += 1

			out = filter2(test["change"])
			self.assertEquals(out, test["expected"], "[%d] Test expected %s, got %s" % (test_counter, test["expected"], out))
