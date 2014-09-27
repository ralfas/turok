def keyboard(banner=None):
	import code, sys

	# use exception trick to pick up the current frame
	try:
		raise None
	except:
		frame = sys.exc_info()[2].tb_frame.f_back

	# evaluate commands in current namespace
	namespace = frame.f_globals.copy()
	namespace.update(frame.f_locals)

	code.interact(banner=banner, local=namespace)

def assertStatsd(testcase, statsd, expected, test_counter, error_message):
	'''
	expected is a dict of 'metric_name : occurences'
	statsd is either a Client or a Pipeline

	Checks that statsd received the expected metrics.
	'''

	stats = statsd._stats

	counters = [metric.split(':')[0] for metric in stats if metric[-1:] == 'c']
	
	if len(expected) != len(counters):
		raise AssertionError(error_message % (test_counter, expected, counters))
	
	counters = sorted(counters)
	expected = sorted(expected)
	testcase.assertListEqual(expected, counters, error_message % (test_counter, expected, counters))
