from gen_thrift.api.ttypes import JoinSource, Source
from joins.risk import user_transactions

from ai.chronon.model import Model, ModelBackend, InferenceSpec, ModelTransforms
from ai.chronon.query import Query, selects

"""
This is the "left side" of the join that will comprise our training set. It is responsible for providing the primary keys
and timestamps for which features will be computed.
"""
source = Source(
    joinSource=JoinSource(
        join=user_transactions.txn_join,
        query=Query(
            selects=selects("user_id"),
        ),
    )
)

model = Model(
    version="1.0",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={"model_type": "xgboost"}
    )
)

v1 = ModelTransforms(
    sources=[source],
    models=[model],
    passthrough_fields=["user_id"],
    version=1
)
