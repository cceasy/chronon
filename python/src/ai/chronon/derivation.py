#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import gen_thrift.api.ttypes as api


def Derivation(name: str, expression: str) -> api.Derivation:
    """
    Derivation allows arbitrary SQL select clauses to be computed using columns from the output
    of a GroupBy or Join. The results will be available both in online fetching response maps
    and in offline Hive tables.

    For Joins, column names are automatically constructed according to the convention:
    `{join_part_prefix}_{group_by_name}_{input_column_name}_{aggregation_operation}_{window}_{by_bucket}`
    (prefix, window, and bucket are optional).

    For ExternalParts, column names follow:
    `ext_{external_source_name}_{value_column}`.

    Note that only values can be used in derivations, not keys. If you want to use a key in the
    derivation, you must define it as a contextual field and refer to it with its prefix included,
    for example: `ext_contextual_request_id`.

    If both name and expression are set to "*", then every raw column will be included along with
    the derived columns.

    :param name: output column name of the SQL expression
    :param expression: any valid Spark SQL select clause based on joinPart or externalPart columns
    :return: a Derivation object representing a single derived column or a wildcard ("*") selection.
    """
    return api.Derivation(name=name, expression=expression)
