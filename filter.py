import json
import re

required_keys = set(['metric', 'aggregation_type', 'start_time', 'resolution', 'datapoints'])
date_format_regex = re.compile('[0-9]{2,2}-[0-9]{2,2}-[0-9]{4,4}')
resolution_format_regex = re.compile('^\d+(\.\d+)?(sec|min)$')

def filter(change):
	"""
	Checks if the change has the required information to be applied.
	"""

	if change is None:
		return None

	try:
		parsed_change = json.loads(change)
	except Exception, e:
		return None

	try:
		_check_required_keys(parsed_change, required_keys)
	except Exception, e:
		return None

	try:
		_check_aggregation_type(parsed_change['aggregation_type'])
	except Exception, e:
		return None

	try:
		_check_time_format(parsed_change['start_time'])
	except Exception, e:
		return None

	try:
		_check_resolution_format(parsed_change['resolution'])
	except Exception, e:
		return None
	
	return parsed_change

def _check_required_keys(change, required_keys):

	keys = set(change.keys())

	if not keys <= required_keys or not required_keys <= keys:
		raise Exception('required keys are missing')

	for key in required_keys:

		if not len(change[key]) > 0:
			raise Exception('%s is missing' % key)

def _check_aggregation_type(agg_type):

	if not agg_type in ['sum', 'average', 'maximum', 'minimum']:
		raise Exception('aggregation_type is unsupported: %s' % agg_type)

def _check_time_format(timestamp):

	search_result = date_format_regex.search(timestamp)
	if search_result == None:
		raise Exception('timestamp is invalid: %s' % timestamp)

	return search_result.group(0)

def _check_resolution_format(resolution):

	search_result = resolution_format_regex.search(resolution)
	if search_result == None:
		raise Exception('resolution is invalid: %s' % resolution)

	return search_result.group(0)
