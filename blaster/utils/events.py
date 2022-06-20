_event_listeners = {}  # {int : [listeners]}

PASS = NOT_HANDLING = object()


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
def broadcast_event(_id, *args, _n=-1, **kwargs):
	listeners = _event_listeners.get(_id)
	if(not listeners):
		return
	listeners_ret_values = []
	# pass handled_count argument to receive
	# previous number of handlers before it receives
	set_arg_handled_count = "handled_count" in kwargs

	# number of handlers handled this event,
	# use case: not handling more than once
	count = 0
	for listener in listeners:
		if(set_arg_handled_count):
			kwargs["handled_count"] = count
		ret = listener(*args, **kwargs)
		if(ret != NOT_HANDLING):
			count += 1

		listeners_ret_values.append(ret)
		if(_n > 0 and _n <= count):
			break

	if(_n == 1):  # special handling when you want just one response, don't return a tuple but just that value
		return listeners_ret_values and listeners_ret_values[0]

	# just fill upto _n values to return
	while(len(listeners_ret_values) < _n):
		listeners_ret_values.append(NOT_HANDLING)

	return tuple(listeners_ret_values)


def add_listener(_id, listener):
	if(not _id or not listener):
		return
	listeners = _event_listeners.get(_id)
	if(not listeners):
		_event_listeners[_id] = listeners = []

	listeners.insert(0, listener)  # at the beginning


def remove_listener(_id, listener):
	listeners = _event_listeners.get(_id)
	if(not listeners):
		return
	if(listener in listeners):
		listeners.remove(listener)


broadcast_event_multiproc = None