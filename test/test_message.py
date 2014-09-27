from unittest import TestCase

from message import Message, from_JSON

class TestMessage(TestCase):

	def test_valid(self):

		tests = [
			{# Empty
				"message" : Message(),
				"expected" : False
			},
			{# Valid
				"message" : Message(
					metric = 'users.registered',
					aggregation_type = 'sum',
					start_time = '01-04-2014 14:35:00',
					resolution = '20sec',
					datapoints = [1.0, 2.0, 3.0]
				),
				"expected" : True
			},
			{# Valid
				"message" : Message(
					metric = 'users.registered',
					aggregation_type = 'maximum',
					start_time = '01-04-2014 14:35:00',
					resolution = '20sec',
					datapoints = [1.0, 2.0, 3.0]
				),
				"expected" : True
			},
			{# Valid
				"message" : Message(
					metric = 'users.registered',
					aggregation_type = 'minimum',
					start_time = '01-04-2014 14:35:00',
					resolution = '20sec',
					datapoints = [1.0, 2.0, 3.0]
				),
				"expected" : True
			},
			{# Valid
				"message" : Message(
					metric = 'users.registered',
					aggregation_type = 'average',
					start_time = '01-04-2014 14:35:00',
					resolution = '20sec',
					datapoints = [1.0, 2.0, 3.0]
				),
				"expected" : True
			},
			{# Missing metric
				"message" : Message(
					metric = None,
					aggregation_type = 'sum',
					start_time = '01-04-2014 14:35:00',
					resolution = '20sec',
					datapoints = [1.0, 2.0, 3.0]
				),
				"expected" : False
			},
			{# Missing aggregation_type
				"message" : Message(
					metric = 'users.registered',
					aggregation_type = None,
					start_time = '01-04-2014 14:35:00',
					resolution = '20sec',
					datapoints = [1.0, 2.0, 3.0]
				),
				"expected" : False
			},
			{# Unsupported aggregation_type
				"message" : Message(
					metric = 'users.registered',
					aggregation_type = 'percentile',
					start_time = '01-04-2014 14:35:00',
					resolution = '20sec',
					datapoints = [1.0, 2.0, 3.0]
				),
				"expected" : False
			},
			{# Missing start_time
				"message" : Message(
					metric = 'users.registered',
					aggregation_type = 'sum',
					start_time = None,
					resolution = '20sec',
					datapoints = [1.0, 2.0, 3.0]
				),
				"expected" : False
			},
			{# Invalid start_time
				"message" : Message(
					metric = 'users.registered',
					aggregation_type = 'sum',
					start_time = '2014-01-04 14:35:00',
					resolution = '20sec',
					datapoints = [1.0, 2.0, 3.0]
				),
				"expected" : False
			},
			{# Missing resolution
				"message" : Message(
					metric = 'users.registered',
					aggregation_type = 'sum',
					start_time = '01-04-2014 14:35:00',
					resolution = None,
					datapoints = [1.0, 2.0, 3.0]
				),
				"expected" : False
			},
			{# Invalid resolution
				"message" : Message(
					metric = 'users.registered',
					aggregation_type = 'sum',
					start_time = '01-04-2014 14:35:00',
					resolution = 'sec',
					datapoints = [1.0, 2.0, 3.0]
				),
				"expected" : False
			},
			{# Invalid resolution
				"message" : Message(
					metric = 'users.registered',
					aggregation_type = 'sum',
					start_time = '01-04-2014 14:35:00',
					resolution = '10',
					datapoints = [1.0, 2.0, 3.0]
				),
				"expected" : False
			},
			{# Missing datapoints
				"message" : Message(
					metric = 'users.registered',
					aggregation_type = 'sum',
					start_time = '01-04-2014 14:35:00',
					resolution = '20sec',
					datapoints = None
				),
				"expected" : False
			},
			{# Empty datapoints
				"message" : Message(
					metric = 'users.registered',
					aggregation_type = 'sum',
					start_time = '01-04-2014 14:35:00',
					resolution = '20sec',
					datapoints = []
				),
				"expected" : False
			}
		]

		test_counter = 0
		for test in tests:
			test_counter += 1

			out = test["message"].valid()
			self.assertEquals(out, test["expected"], "[%d] Test expected %s, got %s" % (test_counter, test["expected"], out))

	def test_from_JSON(self):

		tests = [
			{# Invalid JSON
				"message" : "{'metric' : 'users.registered', 'aggregation_type' : 'sum', 'start_time' : '01-04-2014 14:35:00', 'resolution' : '20sec', 'datapoints' : [1]}",
				"expected" : Exception('Invalid JSON.')
			},
			{# Missing values
				"message" : '{"metric" : "users.registered", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : null}',
				"expected" : Exception('Invalid data.')
			},
			{# Valid
				"message" : '{"metric" : "users.registered", "aggregation_type" : "sum", "start_time" : "01-04-2014 14:35:00", "resolution" : "20sec", "datapoints" : [1.0, 2.0, 3.0]}',
				"expected" : Message(metric = u"users.registered", aggregation_type = u"sum", start_time = u"01-04-2014 14:35:00", resolution = u"20sec", datapoints = [1.0, 2.0, 3.0])
			}
		]

		test_counter = 0
		for test in tests:
			test_counter += 1

			try:
				out = from_JSON(test["message"])
			except Exception, e:
				out = e.args
				test["expected"] = test["expected"].args

			self.assertEquals(out, test["expected"], "[%d] Test expected %s, got %s" % (test_counter, test["expected"], out))
