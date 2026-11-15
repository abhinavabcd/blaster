"""Backend-independent asyncio executor and generated ORM facades.

orms.sqlite_async and orms.pg_async supply only their synchronous model loader.
All scheduling, initialization caching, record proxies, queries and leases live
here. There are no database-driver or ORM imports in this module.

Use a backend AsyncAdapter, or AsyncAdapter(initializer=your_sync_loader)::

    async with AsyncAdapter(initializer=initialize_model) as adapter:
        User = await adapter.initialize(SyncUser)  # generated/cached once
        user = await User.get("alice")
        user.name = "Alice"
        await user.commit()
        async for record in User.query():
            print(record.to_dict())
        async with user.lock():
            await user.reload()

initialize(A, B) returns cached facade classes as a tuple. model(A) binds an
already initialized class without I/O. Constructors, fields, tracked containers,
to_dict(), custom methods and event registration remain synchronous. The known
database entry points get/create_table and commit/update/delete/reload/unlock
are generated once per model. query supports await and async for. Proxies are
not subclasses of the sync model; User.sync_model and user.sync expose it.

Callbacks receive sync records and must stay synchronous. Use adapter.transaction
with a synchronous body and a node exposing transaction(). The whole body runs
on one worker. Model arguments/results are unwrapped/wrapped across that boundary.
Nodes own database rollback and model-state restoration; the adapter duplicates
neither. Cross-node atomicity depends on the backend and is not implied.

run() is also available for arbitrary sync callables. It propagates contextvars,
materializes iterators on the worker and bounds workers with max_workers. Awaited
facade queries currently materialize rows. Do not share live connections/cursors
with the event loop, or concurrently mutate the same record from several tasks.

Cancellation waits for accepted work to settle; it cannot undo a completed commit.
close() drains the executor and does not close caller-owned nodes. Use one event
loop per adapter. Async executors use native threads; cooperative runtimes can
patch the synchronous core in their own processes. Models and database nodes
never need asyncio runtime branches.
"""
import asyncio
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
import contextvars
import functools
import inspect
import time
import weakref

__all__ = ['AsyncAdapter']
_INSIDE_ADAPTER = contextvars.ContextVar('orm_async_adapter', default=None)


def _sync_result(value):
    if inspect.isawaitable(value):
        if inspect.iscoroutine(value):
            value.close()
        raise TypeError('AsyncAdapter requires synchronous operations and transaction callbacks')
    return list(value) if isinstance(value, Iterator) else value


async def _settle(future):
    """Keep accepted work alive and observe its result even after cancellation."""
    try:
        return await asyncio.shield(future)
    except asyncio.CancelledError:
        while not future.done():
            try:
                await asyncio.shield(future)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        if not future.cancelled():
            future.exception()  # consume a failure without replacing cancellation
        raise


class AsyncAdapter:
    """Adapt synchronous callables without changing their model/database APIs.

    One adapter may serve several nodes. max_workers bounds parallel blocking
    calls; database nodes retain their own pooling, batching and transaction rules.
    Use one event loop per adapter. No gevent imports, async drivers or ORM method
    copies are needed here.
    """
    def __init__(self, max_workers=8, *, initializer=None):
        self._initializer = initializer
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='orm-adapter')
        self._loop = None
        self._accepting = True
        self._shutdown = None
        self._models = {}
        self._initializers = {}
        self._instances = weakref.WeakValueDictionary()

    def _check_loop(self):
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise RuntimeError('An AsyncAdapter belongs to one asyncio event loop')
        if _INSIDE_ADAPTER.get() is self:
            raise RuntimeError('Call synchronous ORM methods directly inside adapter operations')
        return loop

    async def run(self, function, /, *args, **kwargs):
        """Run one sync callable; materialize iterator results before returning."""
        loop = self._check_loop()
        if not self._accepting:
            raise RuntimeError('AsyncAdapter is closed')
        if inspect.iscoroutinefunction(function) or inspect.isasyncgenfunction(function):
            raise TypeError('AsyncAdapter requires a synchronous callable')
        context = contextvars.copy_context()
        def invoke():
            marker = _INSIDE_ADAPTER.set(self)
            try:
                return _sync_result(function(*args, **kwargs))
            finally:
                _INSIDE_ADAPTER.reset(marker)
        future = self._executor.submit(context.run, invoke)
        return await _settle(asyncio.wrap_future(future, loop=loop))

    def model(self, sync_model):
        """Bind an initialized sync model once; return its cached async facade.

        Use initialize() to load metadata/schema on a worker as well. The original
        class is never modified. User.sync_model and user.sync expose the original
        sync class/record for synchronous callbacks and transaction bodies.
        """
        if not self._accepting:
            raise RuntimeError('AsyncAdapter is closed')
        if isinstance(sync_model, _AsyncModelType):
            if sync_model._adapter is not self:
                raise ValueError('This model facade belongs to another adapter')
            return sync_model
        if not inspect.isclass(sync_model) or not all(
            callable(getattr(sync_model, name, None)) for name in ('get', 'query', 'commit', 'create_table')
        ):
            raise TypeError('Expected a synchronous ORM model class')
        if sync_model not in self._models:
            methods = {
                'sync_model': sync_model, '_adapter': self, '__slots__': (),
                '__module__': sync_model.__module__, '__doc__': sync_model.__doc__,
            }
            for name in ('get', 'create_table'):
                methods[name] = _class_method(sync_model, name)
            for name in ('commit', 'update', 'delete', 'reload', 'unlock'):
                methods[name] = _instance_method(sync_model, name)
            self._models[sync_model] = _AsyncModelType(sync_model.__name__, (_AsyncRecord,), methods)
        return self._models[sync_model]

    async def initialize(self, *models, initializer=None):
        """Initialize models once and return their cached async facade classes.

        User = await adapter.initialize(SyncUser)
        User, Address = await adapter.initialize(SyncUser, SyncAddress)

        Concurrent/repeated calls share one initialization task per model. The
        loader is supplied by the backend adapter, the constructor's initializer,
        or this call's initializer. Failed initialization may be retried.
        """
        self._check_loop()
        if not models:
            raise ValueError('Supply at least one model')
        facades = tuple(self.model(model) for model in models)
        for facade in facades:
            model = facade.sync_model
            task = self._initializers.get(model)
            if task is None:
                loader = initializer if initializer is not None else self._initializer
                if loader is None:
                    raise ValueError('Supply a synchronous model initializer')
                task = asyncio.create_task(self.run(loader, model))
                self._initializers[model] = task
            try:
                await _settle(task)
            except BaseException:
                if task.done() and (task.cancelled() or task.exception() is not None):
                    if self._initializers.get(model) is task:
                        self._initializers.pop(model, None)
                raise
        return facades[0] if len(facades) == 1 else facades

    def _wrap(self, value):
        facade = self._models.get(type(value))
        if facade is not None:
            key = id(value)
            proxy = self._instances.get(key)
            if proxy is None or proxy.sync is not value:
                proxy = object.__new__(facade)
                object.__setattr__(proxy, '_record', value)
                self._instances[key] = proxy
            return proxy
        if isinstance(value, list):
            return [self._wrap(item) for item in value]
        if isinstance(value, tuple):
            return tuple(self._wrap(item) for item in value)
        if isinstance(value, dict):
            return {key: self._wrap(item) for key, item in value.items()}
        return value

    async def _call_model(self, function, args, kwargs):
        result = await self.run(function, *_unwrap(args), **_unwrap(kwargs))
        return self._wrap(result)

    async def transaction(self, db, function, /, *args, **kwargs):
        """Run an entire synchronous transaction body on one worker thread."""
        def invoke():
            with db.transaction():
                return _sync_result(function(*_unwrap(args), **_unwrap(kwargs)))
        return self._wrap(await self.run(invoke))

    async def close(self):
        """Finish accepted work and shut down workers; safe to call repeatedly."""
        self._check_loop()
        if self._shutdown is None:
            self._accepting = False
            finish = functools.partial(self._executor.shutdown, wait=True)
            self._shutdown = asyncio.create_task(asyncio.to_thread(finish))
        await _settle(self._shutdown)

    async def __aenter__(self):
        self._check_loop()
        if not self._accepting:
            raise RuntimeError('AsyncAdapter is closed')
        return self

    async def __aexit__(self, *exc):
        await self.close()


def _unwrap(value):
    if isinstance(value, _AsyncRecord):
        return value.sync
    if isinstance(value, _AsyncModelType):
        return value.sync_model
    if isinstance(value, list):
        return [_unwrap(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_unwrap(item) for item in value)
    if isinstance(value, dict):
        return {key: _unwrap(item) for key, item in value.items()}
    return value


def _class_method(model, name):
    function = getattr(model, name)
    @functools.wraps(getattr(function, '__func__', function))
    async def method(cls, *args, **kwargs):
        return await cls._adapter._call_model(function, args, kwargs)
    return classmethod(method)


def _instance_method(model, name):
    @functools.wraps(getattr(model, name))
    async def method(self, *args, **kwargs):
        return await self._adapter._call_model(getattr(self.sync, name), args, kwargs)
    return method


class _AsyncModelType(type):
    def __call__(cls, *args, **kwargs):
        if not cls._adapter._accepting:
            raise RuntimeError('AsyncAdapter is closed')
        return cls._adapter._wrap(cls.sync_model(*_unwrap(args), **_unwrap(kwargs)))

    def __getattr__(cls, name):
        return getattr(cls.sync_model, name)


class _AsyncRecord(metaclass=_AsyncModelType):
    __slots__ = ('_record', '__weakref__')

    @property
    def sync(self):
        """The underlying sync record; callbacks/transaction bodies use this API."""
        return self._record

    def __getattr__(self, name):
        return getattr(self.sync, name)

    def __setattr__(self, name, value):
        setattr(self.sync, name, _unwrap(value))

    def __delattr__(self, name):
        delattr(self.sync, name)

    def __repr__(self):
        return f'<{type(self).__name__} async facade for {self.sync!r}>'

    @classmethod
    def query(cls, *args, **kwargs):
        return _AsyncQuery(cls._adapter, cls.sync_model.query, args, kwargs)

    def lock(self, name='', timeout=5000, silent=False, can_hold_until=120000):
        return _AsyncLease(self, name, timeout, silent, can_hold_until)


class _AsyncQuery:
    """Re-execute on each await/iteration; materialize on the adapter's worker."""
    def __init__(self, adapter, function, args, kwargs):
        self.adapter, self.function = adapter, function
        self.args, self.kwargs = args, kwargs

    def __await__(self):
        return self.adapter._call_model(self.function, self.args, self.kwargs).__await__()

    async def __aiter__(self):
        for item in await self:
            yield item


class _AsyncLease:
    """Poll the sync lock outside workers so a waiting lease cannot starve unlock."""
    def __init__(self, record, name, timeout, silent, can_hold_until):
        self.record, self.name = record, name
        self.timeout, self.silent, self.can_hold_until = timeout, silent, can_hold_until
        self._handle = None

    def __bool__(self):
        return self._handle is not None

    async def acquire(self):
        if self._handle is not None:
            return self
        deadline = time.monotonic() + self.timeout / 1000
        def attempt():
            handle = self.record.sync.lock(name=self.name, timeout=1, silent=True,
                                           can_hold_until=self.can_hold_until)
            # Store before delivering the result, so cancellation can release a
            # lease acquired by a job whose caller has already been cancelled.
            if handle:
                self._handle = handle
        try:
            while time.monotonic() < deadline:
                await self.record._adapter.run(attempt)
                if self._handle is not None:
                    return self
                await asyncio.sleep(min(.01, max(0, deadline - time.monotonic())))
        except asyncio.CancelledError:
            if self._handle is not None:
                cleanup = asyncio.create_task(self.unlock())
                try:
                    await _settle(cleanup)
                except asyncio.CancelledError:
                    pass
            raise
        if not self.silent:
            raise TimeoutError('Record lock timed out')
        return self

    def __await__(self):
        return self.acquire().__await__()

    async def unlock(self, force=False):
        if self._handle is not None:
            handle = self._handle
            def release():
                handle.unlock(force=force)
                if self._handle is handle:
                    self._handle = None
            await self.record._adapter.run(release)

    async def __aenter__(self):
        await self.acquire()
        if not self:
            raise TimeoutError('Lock not acquired')
        return self.record

    async def __aexit__(self, *exc):
        await self.unlock()
