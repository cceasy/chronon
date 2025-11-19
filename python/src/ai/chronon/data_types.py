"""
Data type utilities for creating TDataType objects in Chronon.

This module provides a convenient interface for creating structured data types
that are used across various Chronon components like joins, models, and external sources.
"""

from typing import List, Tuple

import gen_thrift.api.ttypes as api

# Type alias for field definitions - list of (field_name, data_type) tuples
FieldsType = List[Tuple[str, api.TDataType]]


class DataType:
    """
    Helper class to generate data types for declaring schema.
    This supports primitive like numerics, string etc., and complex
    types like Map, List, Struct etc.
    """

    BOOLEAN = api.TDataType(api.DataKind.BOOLEAN)
    SHORT = api.TDataType(api.DataKind.SHORT)
    INT = api.TDataType(api.DataKind.INT)
    LONG = api.TDataType(api.DataKind.LONG)
    FLOAT = api.TDataType(api.DataKind.FLOAT)
    DOUBLE = api.TDataType(api.DataKind.DOUBLE)
    STRING = api.TDataType(api.DataKind.STRING)
    BINARY = api.TDataType(api.DataKind.BINARY)

    # Types unsupported by Avro. See AvroConversions.scala#fromChrononSchema
    # BYTE = api.TDataType(api.DataKind.BYTE)
    # DATE = api.TDataType(api.DataKind.DATE)
    # TIMESTAMP = api.TDataType(api.DataKind.TIMESTAMP)

    def MAP(key_type: api.TDataType, value_type: api.TDataType) -> api.TDataType:
        assert key_type == api.TDataType(api.DataKind.STRING), (
            "key_type has to be STRING for MAP types"
        )

        return api.TDataType(
            api.DataKind.MAP,
            params=[api.DataField("key", key_type), api.DataField("value", value_type)],
        )

    def LIST(elem_type: api.TDataType) -> api.TDataType:
        return api.TDataType(api.DataKind.LIST, params=[api.DataField("elem", elem_type)])

    def STRUCT(name: str, *fields: FieldsType) -> api.TDataType:
        return api.TDataType(
            api.DataKind.STRUCT,
            params=[api.DataField(name, data_type) for (name, data_type) in fields],
            name=name,
        )
