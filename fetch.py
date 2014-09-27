from message import from_JSON

MAX_ITEMS = 10

def fetch(items, queue, statsd):

	messages = []

	q_messages = queue.get_messages(num_messages=items)

	if len(q_messages) == 0:
		statsd.incr('fetch.empty.count')
	else:
		statsd.incr('fetch.not_empty.count')

	for q_m in q_messages:

		try:
			m = from_JSON(q_m.get_body())
		except Exception, e:
			# invalid JSON or values
			if str(e) == 'Invalid JSON.':
				statsd.incr('fetch.invalid.json.count')
				continue
			elif str(e) == 'Invalid data.':
				statsd.incr('fetch.invalid.values.count')
				continue
			else:
				raise e

		messages.append(m)
		statsd.incr('fetch.items.count')

	return messages