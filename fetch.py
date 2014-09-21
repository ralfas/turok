from message import from_JSON

MAX_ITEMS = 10

def fetch(items, queue):

	messages = []
	for q_m in queue.get_messages(num_messages=items):

		try:
			m = from_JSON(q_m.get_body())
		except Exception, e:
			# invalid JSON or values
			if str(e) == 'Invalid JSON.' or str(e) == 'Invalid data.':
				continue
			else:
				raise e

		messages.append(m)

	return messages