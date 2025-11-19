import inspect
from dataclasses import dataclass
from typing import Dict, List, Optional

import gen_thrift.api.ttypes as ttypes

from ai.chronon import utils
from ai.chronon.data_types import DataType, FieldsType
from ai.chronon.utils import ANY_SOURCE_TYPE, normalize_source


class ModelBackend:
    VERTEXAI = ttypes.ModelBackend.VertexAI
    SAGEMAKER = ttypes.ModelBackend.SageMaker


@dataclass
class ResourceConfig:
    min_replica_count: Optional[int] = None
    max_replica_count: Optional[int] = None
    machine_type: Optional[str] = None

    def to_thrift(self):
        return ttypes.ResourceConfig(
            minReplicaCount=self.min_replica_count,
            maxReplicaCount=self.max_replica_count,
            machineType=self.machine_type,
        )


@dataclass
class InferenceSpec:
    model_backend: Optional[ModelBackend] = None
    model_backend_params: Optional[Dict[str, str]] = None
    resource_config: Optional[ResourceConfig] = None

    def to_thrift(self):
        resource_config_thrift = None
        if self.resource_config:
            resource_config_thrift = self.resource_config.to_thrift()
        
        return ttypes.InferenceSpec(
            modelBackend=self.model_backend,
            modelBackendParams=self.model_backend_params,
            resourceConfig=resource_config_thrift,
        )


def Model(
    version: str,
    inference_spec: Optional[InferenceSpec] = None,
    input_mapping: Optional[Dict[str, str]] = None,
    output_mapping: Optional[Dict[str, str]] = None,
    value_fields: Optional[FieldsType] = None,
    output_namespace: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
    tags: Optional[Dict[str, str]] = None,
) -> ttypes.Model:
    """
    Creates a Model object for ML model inference and orchestration.

    :param version:
        Version string for the model configuration
    :type version: str
    :param inference_spec:
        Model + model backend specific details necessary to perform inference
    :type inference_spec: InferenceSpec
    :param input_mapping:
        Spark SQL queries to transform input data to the format expected by the model
    :type input_mapping: Dict[str, str]
    :param output_mapping:
        Spark SQL queries to transform model output to desired output format
    :type output_mapping: Dict[str, str]
    :param value_fields:
        List of tuples of (field_name, DataType) defining the schema of the model's output values.
        If provided, creates a STRUCT schema that will be set as the model's valueSchema.
        Example: [('score', DataType.DOUBLE), ('category', DataType.STRING)]
    :type value_fields: FieldsType
    :param output_namespace:
        Namespace for the model output
    :type output_namespace: str
    :param table_properties:
        Additional table properties for the model output
    :type table_properties: Dict[str, str]
    :param tags:
        Additional metadata that does not directly affect computation, but is useful for management.
    :type tags: Dict[str, str]
    :return:
        A Model object
    """
    # Get caller's filename to assign team
    team = inspect.stack()[1].filename.split("/")[-2]

    assert isinstance(version, str), (
        f"Version must be a string, but found {type(version).__name__}"
    )

    # Create metadata
    meta_data = ttypes.MetaData(
        outputNamespace=output_namespace,
        team=team,
        tags=tags,
        tableProperties=table_properties,
        version=version,
    )

    # Convert inference_spec to thrift if provided
    inference_spec_thrift = None
    if inference_spec:
        inference_spec_thrift = inference_spec.to_thrift()

    # Create value schema if value_fields are provided
    value_schema = None
    if value_fields:
        schema_name = "model_value_schema"
        value_schema = DataType.STRUCT(schema_name, *value_fields)
    
    # Create and return the Model object
    model = ttypes.Model(
        metaData=meta_data,
        inferenceSpec=inference_spec_thrift,
        inputMapping=input_mapping,
        outputMapping=output_mapping,
        valueSchema=value_schema,
    )

    return model


def _get_model_transforms_output_table_name(model_transforms: ttypes.ModelTransforms, full_name: bool = False):
    """Generate output table name for ModelTransforms"""
    utils.__set_name(model_transforms, ttypes.ModelTransforms, "models")
    return utils.output_table_name(model_transforms, full_name=full_name)

def ModelTransforms(
    sources: List[ANY_SOURCE_TYPE],
    models: List[ttypes.Model],
    version: int,
    passthrough_fields: Optional[List[str]] = None,
    key_fields: Optional[FieldsType] = None,
    output_namespace: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
    tags: Optional[Dict[str, str]] = None,
) -> ttypes.ModelTransforms:
    """
    ModelTransforms allows taking the output of existing sources (Event/Entity/Join) and
    enriching them with 1 or more model outputs. This can be used in GroupBys, Joins, or hit directly
    via the fetcher. The GroupBy path allows for async materialization of model outputs to the online KV store for low latency
    serving. The fetcher path allows for on-demand model inference during online serving (at the cost of higher latency / more
    model inference calls).

    Attributes:
     - sources: List of existing sources (Event/Entity/Join sources) to be enriched with model outputs
     - models: List of Model objects that will be used for inference on the source data
     - passthrough_fields: Fields from the source that we want to passthrough alongside the model outputs
    - key_fields: List of tuples of (field_name, DataType) defining the schema of the key fields.
        If provided, creates a STRUCT schema that will be set as the ModelTransforms' keySchema.
        Example: [('user_id', DataType.STRING), ('session_id', DataType.STRING)]
     - output_namespace: Namespace for the model output
     - table_properties: Additional table properties for the model output
     - tags: Additional metadata tags
    """
    # Get caller's filename to assign team
    team = inspect.stack()[1].filename.split("/")[-2]
    
    # Normalize all sources to ensure they are properly wrapped
    normalized_sources = [normalize_source(source) for source in sources]
    
    # Create metadata
    meta_data = ttypes.MetaData(
        outputNamespace=output_namespace,
        team=team,
        tags=tags,
        tableProperties=table_properties,
        version=str(version),
    )
    
    # Create key schema if key_fields are provided
    key_schema = None
    if key_fields:
        schema_name = "modeltransform_key_schema"
        key_schema = DataType.STRUCT(schema_name, *key_fields)

    model_transforms = ttypes.ModelTransforms(
        sources=normalized_sources,
        models=models,
        passthroughFields=passthrough_fields,
        metaData=meta_data,
        keySchema=key_schema,
    )
    
    # Add the table property for output table name generation
    model_transforms.__class__.table = property(lambda self: _get_model_transforms_output_table_name(self, full_name=True))
    
    return model_transforms
