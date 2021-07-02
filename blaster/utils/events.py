_event_listeners = {} # {int : [listeners]}


def register_as_listener(_id):
	if(callable(_id)):
		original_func = getattr(_id, "_original", _id)
		add_listener(original_func.__name__, _id)
		return _id
	else:
		def decorator(func):
			add_listener(_id, func)
			return func
		return decorator

def broadcast_event(_id, *args, **kwargs):
	listeners = _event_listeners.get(_id)
	if(not listeners):
		return
	count = 0
	for listener in listeners:
		if(listener and listener(*args, **kwargs)):
			count += 1
	return count

def add_listener(_id, listener):
	listeners = _event_listeners.get(_id)
	if(not listeners):
		_event_listeners[id] = listeners = []

	listeners.insert(0, listener)

def remove_listener(_id, listener):
	listeners = _event_listeners.get(_id)
	if(not listeners):
		return
	if(listener in listeners):
		listeners.remove(listener)
