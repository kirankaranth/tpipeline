# pylint: skip-file

from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *
from prophecy.cb.ui.UISpecUtil import getColumnsToHighlight2, validateSColumn

from prophecy.cb.util.StringUtils import isBlank


class SnapshotFromHistory(ComponentSpec):
    name: str = "SnapshotFromHistory"
    category: str = "Transform"
    gemDescription: str = "SnapshotFromHistory"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class SnapshotFromHistoryProperties(ComponentProperties):
        columnsSelector: List[str] = field(default_factory=list)
        snapshot_date: SColumn = SColumn("")
        history_key: SString = SString("")
        keep_deleted: bool = False
        snapshot_date_format: SString = SString("")
        columns_to_keep: SString = SString("")
        max_value_field: SString = SString("")

    def validate(
        self,
        context: WorkflowContext,
        component: Component[SnapshotFromHistoryProperties],
    ) -> List[Diagnostic]:
        return validateSColumn(
            component.properties.snapshot_date, "snapshot_date", component
        )

    def onChange(
        self,
        context: WorkflowContext,
        oldState: Component[SnapshotFromHistoryProperties],
        newState: Component[SnapshotFromHistoryProperties],
    ) -> Component[SnapshotFromHistoryProperties]:
        return newState

    def dialog(self) -> Dialog:
        return Dialog("SnapshotFromHistory").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                PortSchemaTabs(
                    selectedFieldsProperty=("columnsSelector")
                ).importSchema()
            )
            .addColumn(
                StackLayout(height="100%")
                .addElement(
                    ExpressionBox("Snapshot Date")
                    .bindPlaceholder("snapshot_date")
                    .bindProperty("snapshot_date.expression")
                )
                .addElement(Checkbox("Keep deleted").bindProperty("keep_deleted"))
                .addElement(
                    ExpressionBox("History Key")
                    .bindPlaceholder("history_key")
                    .bindProperty("history_key")
                )
                .addElement(
                    ExpressionBox("Columns To Keep")
                    .bindPlaceholder("columns_to_keep")
                    .bindProperty("columns_to_keep")
                )
                .addElement(
                    ExpressionBox("Max value field")
                    .bindPlaceholder("max_value_field")
                    .bindProperty("max_value_field")
                )
                .addElement(
                    ExpressionBox("Snapshot Date Format")
                    .bindPlaceholder("snapshot_date_format")
                    .bindProperty("snapshot_date_format")
                )
            )
        )

    class SnapshotFromHistoryCode(ComponentCode):
        def __init__(self, newProps):
            self.props: SnapshotFromHistory.SnapshotFromHistoryProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            import pyspark.sql.functions as pf
            import json

            def snapshot_from_history(spark: SparkSession, params: dict) -> dict:
                result = {}
                if "source" in params:
                    source_df: DataFrame = params["source"][0]
                else:
                    raise Exception("no source")

                target_df: DataFrame = params["source"][0]
                mandatory_columns = "EDWH_PEIL_DTM,EDWH_PROCES_DTD,EDWH_VERWIJDER_DTD"
                columns_to_keep = params["columns_to_keep"]
                if "history_key" in params:
                    history_key = params.get("history_key")
                else:
                    raise Exception("no history_key")
                max_value_field = ""
                if "max_value_field" in params:
                    max_value_field = params.get("max_value_field")
                keep_deleted = False
                if "keep_deleted" in params:
                    keep_deleted = params.get("keep_deleted")
                filter_string = ""
                if "filter_string" in params:
                    filter_string = params.get("filter_string")
                snapshot_date = ""
                if "snapshot_date" in params:
                    snapshot_date = params["snapshot_date"]
                else:
                    raise Exception("no snapshot_date")
                snapshot_date_format = "yyyy-MM-dd"
                if "snapshot_date_format" in params:
                    snapshot_date_format = params.get("snapshot_date_format")

                # filter by snapshot date
                # snapshot_date = get_params(snapshot_date)
                # try:
                # snapshot_date = to_date(lit(snapshot_date), snapshot_date_format)
                target_df = source_df.filter(
                    coalesce(col("EDWH_PEIL_DTM"), lit("0001-01-01").cast("date"))
                    <= snapshot_date
                )
                # except:
                #     raise Exception("Invalid snapshot_date")
                # extract max value, if required
                if max_value_field:
                    result["max_value"] = target_df.agg(
                        max(max_value_field).alias(max_value_field)
                    ).first()[max_value_field]
                    max_value = result["max_value"] if result["max_value"] else 0
                    # print(max_value)
                    # setattr(Config, "SNAPSHOT_FROM_HISTORY_MAX_VALUE", max_value)

                    if (
                        hasattr(Config, "SNAPSHOT_FROM_HISTORY_MAX_VALUE")
                        and Config.SNAPSHOT_FROM_HISTORY_MAX_VALUE
                    ):
                        snapshot_from_history_max_value = {}
                        if isinstance(
                            Config.SNAPSHOT_FROM_HISTORY_MAX_VALUE, ConfigTree
                        ):
                            snapshot_from_history_max_value = dict(
                                Config.SNAPSHOT_FROM_HISTORY_MAX_VALUE
                            )
                        elif isinstance(Config.SNAPSHOT_FROM_HISTORY_MAX_VALUE, str):
                            snapshot_from_history_max_value = json.loads(
                                Config.SNAPSHOT_FROM_HISTORY_MAX_VALUE
                            )
                    else:
                        snapshot_from_history_max_value = {}
                    # print(snapshot_from_history_max_value)
                    snapshot_from_history_max_value[
                        f"MAX_{max_value_field.upper()}"
                    ] = max_value
                    setattr(
                        Config,
                        "SNAPSHOT_FROM_HISTORY_MAX_VALUE",
                        json.dumps(snapshot_from_history_max_value),
                    )
                    # print("Updated Config.SNAPSHOT_FROM_HISTORY_MAX_VALUE:")
                    # print(Config.SNAPSHOT_FROM_HISTORY_MAX_VALUE)

                # sort rows within key by descending EDWH_PEIL_DTM,EDWH_PROCES_DTD,EDWH_VERWIJDER_DTD and all other columns
                df_columns = sorted([c.upper() for c in target_df.columns])

                mandatory_columns_list = mandatory_columns.upper().split(",")
                mandatory_columns_list = [c.strip() for c in mandatory_columns_list]

                sorted_df_columns = sorted(
                    [x for x in df_columns if x not in mandatory_columns_list]
                )
                order_columns = mandatory_columns_list + sorted_df_columns

                history_keys = history_key.split(",")
                history_keys = [c.strip() for c in history_keys]

                window_spec = Window.partitionBy(history_keys).orderBy(
                    [pf.col(c).desc() for c in order_columns]
                )

                # keep only first row in the assorted key
                target_df = (
                    target_df.withColumn("row_number", row_number().over(window_spec))
                    .where(
                        "row_number == 1 and (EDWH_VERWIJDER_DTD is null or "
                        + ("1=1" if keep_deleted else "1=2")
                        + ")"
                    )
                    .drop("row_number")
                )

                # apply additional filter if required
                if filter_string:
                    target_df = target_df.filter(filter_string)

                # disabled as it was in SAS
                # result["target"] = [target_df.select(columns_to_keep.split(","))]
                result["target"] = [target_df]

                result["is_error"] = False

                return result

            params = {
                "source": [in0],
                "history_key": self.props.history_key,
                "columns_to_keep": self.props.columns_to_keep,
                "keep_deleted": self.props.keep_deleted,
                "snapshot_date": self.props.snapshot_date.column(),
                "snapshot_date_format": self.props.snapshot_date_format,
                "max_value_field": self.props.max_value_field,
            }

            result = snapshot_from_history(spark, params)
            return result["target"][0]
