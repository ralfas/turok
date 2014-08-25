MAX_ITEMS = 10

def fetch(items, queue):

	return queue.get_messages(num_messages=items)