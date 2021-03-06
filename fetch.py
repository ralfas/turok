from message import from_JSON

MAX_ITEMS = 10

def fetch(items, queue, statsd):

	messages = []

	q_messages = queue.get_messages(num_messages=items)
	statsd.incr('fetch.sqs.get_messages')

	if len(q_messages) == 0:
		statsd.incr('fetch.empty')
	else:
		statsd.incr('fetch.not_empty')

	for q_m in q_messages:

		try:
			m = from_JSON(q_m.get_body())
		except Exception, e:
			# invalid JSON or values
			if str(e) == 'Invalid JSON.':
				statsd.incr('fetch.invalid.json')
				continue
			elif str(e) == 'Invalid data.':
				statsd.incr('fetch.invalid.values')
				continue
			else:
				raise e

		messages.append(m)
		statsd.incr('fetch.items')

	return messages