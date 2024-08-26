_event_listeners = {}  # {int : [listeners]}

NOT_HANDLING_EVENT = object()


def add_listener(_id, listener):
	if(not _id or not listener):
		return
	listeners = _event_listeners.get(_id)
	if(not listeners):
		_event_listeners[_id] = listeners = []

	# last added first to be called
	# we do this because first level listeners are last to listen
	listeners.insert(0, listener)


def remove_listener(_id, listener):
	listeners = _event_listeners.get(_id)
	if(not listeners):
		return
	if(listener in listeners):
		listeners.remove(listener)


def register_listener(_id, name=None):
	if(callable(_id)):
		# id is func here
		original_func = getattr(_id, "_original", _id)
		add_listener(name or original_func.__name__, _id)
		return _id
	else:
		# id is a string/list, probably using as a decorator
		def decorator(func):
			if(isinstance(_id, list)):
				for _id_item in _id:
					add_listener(_id_item, func)
			else:
				add_listener(_id, func)
			return func
		return decorator


# _n => max number of broadcasts, -1 => to all possible
def broadcast_event_iter(_id, *args, **kwargs):
	listeners = _event_listeners.get(_id)
	if(not listeners):
		return

	# number of handlers handled this event,
	# use case: not handling more than once
	for listener in listeners:
		ret = listener(*args, **kwargs)
		if(ret is not NOT_HANDLING_EVENT):  # special object
			yield ret


# _n => max number of broadcasts, -1 => to all possible
def broadcast_event(_id, *args, **kwargs):
	return tuple(broadcast_event_iter(_id, *args, **kwargs))


broadcast_event_multiproc = None
