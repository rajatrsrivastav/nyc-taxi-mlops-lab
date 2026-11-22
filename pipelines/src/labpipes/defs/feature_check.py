"""Block training when the warehouse feature table drifts from its frozen contract."""

import dagster as dg

from ..contracts import features_contract
from ..partitions import check_month, trip_months
from ..resources import RedshiftData
from .features import trip_features


def feature_schema(redshift: RedshiftData) -> tuple[list[list[str]], list[list[str]]]:
    contract = features_contract()
    columns = redshift.query(
        "select column_name, data_type, character_maximum_length "
        "from information_schema.columns where table_schema = 'staging' "
        "and table_name = 'trip_features' order by ordinal_position"
    )
    expected = [[column["name"], column["type"]] for column in
                [*contract["keys"], *contract["inputs"], contract["label"]]]
    actual = [
        [column["column_name"],
         f"char({column['character_maximum_length']})"
         if column["data_type"] in {"character", "char", "bpchar"} else column["data_type"]]
        for column in columns
    ]
    return expected, actual


def feature_value_violations(redshift: RedshiftData, month: str) -> int:
    contract = features_contract()
    invalid = []
    for column in [*contract["inputs"], contract["label"]]:
        name = column["name"]
        low, high = column["range"]
        lower_operator = "<=" if column.get("exclusive_min") else "<"
        invalid.append(f"({name} is null or {name} {lower_operator} {low} or {name} > {high})")
    return int(redshift.scalar(
        f"select count(*) from staging.trip_features where trip_month = '{check_month(month)}' "
        f"and ({' or '.join(invalid)})"
    ))


@dg.asset_check(asset=trip_features, partitions_def=trip_months, blocking=True, pool="redshift")
def feature_contract(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    expected, actual = feature_schema(redshift)
    if actual != expected:
        return dg.AssetCheckResult(
            passed=False, description="The feature table columns differ from the frozen contract.",
            metadata={"expected": expected, "actual": actual},
        )
    invalid = feature_value_violations(redshift, context.partition_key)
    return dg.AssetCheckResult(passed=invalid == 0, metadata={"invalid_rows": invalid})
