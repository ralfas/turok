import json
import re

required_keys = set(['metric', 'aggregation_type', 'start_time', 'resolution', 'datapoints'])
date_format_regex = re.compile('[0-9]{2,2}-[0-9]{2,2}-[0-9]{4,4}')
resolution_format_regex = re.compile('^\d+(\.\d+)?(sec|min)$')

class Message(object):

	def __init__(self, metric=None, datapoints=None, start_time=None, resolution=None, aggregation_type=None):

		self.metric = metric
		self.datapoints = datapoints
		self.start_time = start_time
		self.resolution = resolution
		self.aggregation_type = aggregation_type
	
	def valid(self):
		"""
		Check if all required fields have been provided.
		"""

		if valid_metric(self.metric) \
		and valid_datapoints(self.datapoints) \
		and valid_start_time(self.start_time) \
		and valid_resolution(self.resolution) \
		and valid_aggregation_type(self.aggregation_type):
			return True

		return False

def valid_metric(metric):
	
	if metric is not None:
		return True

	return False

def valid_datapoints(datapoints):
	
	if datapoints is not None \
	and type(datapoints) is list \
	and len(datapoints) > 0:
		return True

	return False

def valid_start_time(start_time):
	
	if start_time is not None \
	and date_format_regex.search(start_time) is not None:
		return True

	return False

def valid_resolution(resolution):

	if resolution is not None \
	and resolution_format_regex.search(resolution) is not None:
		return True

	return False

def valid_aggregation_type(aggregation_type):
	if aggregation_type in ['minimum', 'maximum', 'average', 'sum']:
		return True

	return False
