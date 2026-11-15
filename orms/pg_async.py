"""PostgreSQL loader for the shared asyncio ORM facade.

    from orms.pg import Model, Attribute, DatabaseNode
    from orms.pg_async import AsyncAdapter
    # Define SyncUser(Model) against a DatabaseNode, then inside async code:
    # async with AsyncAdapter(create_tables=True) as adapter:
    #     User = await adapter.initialize(SyncUser)
    #     user = await User.get("alice")
    #     await user.commit()

create_tables defaults to the synchronous ORM's production schema checks; True
opts into table creation. The sync ORM owns pooling, SQL, transactions and change
tracking. async_wrapper owns all asyncio adaptation. Callbacks/transaction bodies
stay synchronous and use User.sync_model or user.sync. Close nodes explicitly.
"""
from functools import partial
from .async_wrapper import AsyncAdapter as _AsyncAdapter

__all__ = ['AsyncAdapter']


class AsyncAdapter(_AsyncAdapter):
    def __init__(self, max_workers=8, *, create_tables=None, initializer=None):
        if initializer is None:
            from .pg import initialize_model
            initializer = partial(initialize_model, create_tables=create_tables)
        super().__init__(max_workers=max_workers, initializer=initializer)
