from boto.dynamodb2.table import Table
from boto.dynamodb2.items import Item
from boto.exception import JSONResponseError
from boto.dynamodb2.exceptions import ItemNotFound

from schema import DynamoDB_Schema

from message import date_format_regex

import json

TABLE_PREFIX = 'turok'
TABLE_JOINER = '_'

def apply(message, connection, statsd):
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

	metric = message.metric
	start_time = message.start_time
	resolution = message.resolution
	datapoints = message.datapoints
	aggregation_type = message.aggregation_type

	table_name = get_table_name(resolution, start_time)

	try:
		table = Table(table_name, connection=connection)
		statsd.incr('apply.dynamodb.table.describe')
		table.describe()

	except JSONResponseError, e:

		if e.error_code == 'ResourceNotFoundException':
			statsd.incr('apply.dynamodb.table.create')
			table = Table.create(table_name, schema=DynamoDB_Schema, connection=connection)
		else:
			raise e

	try:
		statsd.incr('apply.dynamodb.table.get_item')
		m = table.get_item(consistent=True, metric=metric, start_time=start_time)

		m['datapoints'] = json.dumps(
			aggregate(json.loads(m['datapoints']), datapoints, aggregation_type, statsd)
		)
		statsd.incr('apply.metric.update')

	except ItemNotFound, e:
		m = Item(table)
		m['metric'] = metric
		m['start_time'] = start_time
		m['datapoints'] = json.dumps(datapoints)
		statsd.incr('apply.metric.create')

	statsd.incr('apply.dynamodb.item.write')
	m.save()

def get_table_name(resolution, start_time):

	matches = date_format_regex.search(start_time)

	if matches is None:
		raise Exception('Invalid start time.')

	return TABLE_PREFIX + TABLE_JOINER + matches.group(0) + TABLE_JOINER + resolution

def aggregate(existing_datapoints, new_datapoints, aggregation_type, statsd):

	counter = 0
	while counter < len(existing_datapoints):

		d = existing_datapoints[counter]
		new_d = new_datapoints[counter]

		# Replace datapoint with new
		if d == None:
			d = new_d
		# Combine datapoint with new
		elif new_d != None:

			if aggregation_type == 'sum':
				d += new_d

			elif aggregation_type == 'average':
				d = (d + new_d) / 2

			elif aggregation_type == 'minimum':
				d = d if d < new_d else new_d

			elif aggregation_type == 'maximum':
				d = d if d > new_d else new_d

		existing_datapoints[counter] = d
		counter += 1

	statsd.incr('apply.aggregate.' + aggregation_type)

	return existing_datapoints