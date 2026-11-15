"""SQLite loader for the shared asyncio ORM facade.

    from orms.sqlite import Model, Attribute, DatabaseNode
    from orms.sqlite_async import AsyncAdapter
    # Define SyncUser(Model) against a DatabaseNode, then inside async code:
    # async with AsyncAdapter() as adapter:
    #     User = await adapter.initialize(SyncUser)
    #     user = await User.get("alice")
    #     await user.commit()

The synchronous implementation lives in orms.sqlite. All async scheduling,
wrapping, cached model generation, queries and leases live in async_wrapper.
Callbacks and explicit transaction bodies use User.sync_model or user.sync.
"""
from .async_wrapper import AsyncAdapter as _AsyncAdapter

__all__ = ['AsyncAdapter']


class AsyncAdapter(_AsyncAdapter):
    def __init__(self, max_workers=8, *, initializer=None):
        if initializer is None:
            from .sqlite import initialize_model
            initializer = initialize_model
        super().__init__(max_workers=max_workers, initializer=initializer)
