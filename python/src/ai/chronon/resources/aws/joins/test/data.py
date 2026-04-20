from group_bys.test.data import group_by_v1

from ai.chronon.types import EventSource, Join, JoinPart, Query, selects

"""
This is the "left side" of the join that will comprise our training set. It is responsible for providing the primary keys
and timestamps for which features will be computed.
"""
source = EventSource(
    table="data.checkouts",
    query=Query(
        selects=selects("user_id"),  # The primary key used to join various GroupBys together
        time_column="ts",
        start_partition="2023-11-01",
    ),
)

v1 = Join(
    left=source,
    right_parts=[JoinPart(group_by=group_by_v1)],
    row_ids="user_id",
    version=0,
)
