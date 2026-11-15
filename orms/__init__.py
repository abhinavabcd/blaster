"""Standalone synchronous ORMs and their shared asyncio facade.

orms.sqlite and orms.pg contain only synchronous database/model APIs. Their
sqlite_async and pg_async loaders share all adaptation code in async_wrapper.
The older sqlite_orm/pg_orm imports remain available for sync consumers. Use one
core import path consistently per application. No server startup is imported.
"""
