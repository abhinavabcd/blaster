# mongo orm
# setup: 2 replica sets
# intialize table with primary and multiple secondary shards

# insert 10000 documents
# 1. verify all 10000 are split to shards
# 2. verify secondary shards are also updated for all 10000 documents

# update 100 documents
# 1. verify  updates are propagated to the secondary shards and are available for query
