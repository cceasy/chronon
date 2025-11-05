from joins.gcp import demo

from ai.chronon.model import Model, ModelBackend, InferenceSpec, ModelTransforms
from ai.chronon.query import Query, selects
from ai.chronon.source import JoinSource

"""
This model takes some of the listing related fields from the demo join and uses that
to build up a couple of listing related embeddings
"""

source = JoinSource(join=demo.v1)

item_description_model = Model(
    version="1.0",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={
            "model_name": "gemini-embedding-001",
            "model_type": "publisher",
        }
    ),
    input_mapping={
        "instance": "named_struct('content', concat(listing_id_headline, '; ', listing_id_long_description)",
        "parameters": "CAST(map() AS MAP<STRING, STRING>)"
    },
    output_mapping={
        "item_embedding": "predictions[0].embeddings.values"
    }
)

item_img_model = Model(
    version="001",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={
            "model_name": "multimodalembedding",
            "model_type": "publisher",
            "version": "001",
            "dimension": "512"
        }
    ),
    input_mapping={
        "instance": "named_struct('image', named_struct('gcsUri', listing_id_main_image_path), 'text','')",
        "parameters": "CAST(map() AS MAP<STRING, STRING>)"
    },
    output_mapping={
         "image_embedding": "predictions[0].imageEmbedding"
    }
)

# Create a listing_model transforms
v1 = ModelTransforms(
    sources=[source],
    models=[item_description_model, item_img_model],
    passthrough_fields=["user_id", "listing_id", "row_id"],
    version=1,
    output_namespace="models"
)
