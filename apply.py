from boto.dynamodb2.table import Table
from boto.dynamodb2.items import Item
from boto.exception import JSONResponseError

from schema import SCHEMA

import json

TABLE_PREFIX = 'turok'
TABLE_DELIMITER = '_'

def apply(metric, start_time, resolution, datapoints, aggregation_type, connection):
	"""
	Applies the specific change.

	New Table behaviour:
	If a DynamoDB Table doesn't already exist for the configured prefix, day and time resolution
	 combination, then a Table will be created.

	New Item behaviour:
	If a DynamoDB Item doesn't already exist for the metric and the configured time period, then
	 one will be created.

	Update Item behaviour:
	If a DynamoDB Item already exists for the metric and configured time period, then the new 
	data points will be merged with the existing data as per the aggregation type specified.

	Aggregation Type behaviour:
	- sum: the values for each measurement will be added together
	- average: the values for each measurement will be added together with the data point already
	 stored and divided by 2
	- minimum: the minimum value will be stored for each measuremenet out of the new value and
	 the existing value
	- maximum: the maximum value will be stored for each measuremenet out of the new value and
	 the existing value
	"""

	table_name = get_table_name(resolution)
	
	try:
		table = Table(table_name, connection=connection)
		table.describe()

	except JSONResponseError, e:

		if e.error_code == 'Cannot do operations on a non-existent table':
			table = Table.create(table_name, schema=SCHEMA, connection=connection)
		else:
			raise e

	encoded_datapoints = json.dumps(datapoints)

	metric = Item(table, data={
		'metric' : metric,
		'start_time' : start_time,
		'datapoints' : encoded_datapoints
	})

	print metric.save()

def get_table_name(resolution):

	return TABLE_PREFIX + TABLE_DELIMITER + resolution