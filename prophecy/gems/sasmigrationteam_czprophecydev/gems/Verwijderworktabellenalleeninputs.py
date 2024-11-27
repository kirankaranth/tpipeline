# pylint: skip-file

from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *


class Verwijderworktabellenalleeninputs(ComponentSpec):
    name: str = "Verwijderworktabellenalleeninputs"
    category: str = "Transform"

    def optimizeCode(self) -> bool:
        return False

    @dataclass(frozen=True)
    class VerwijderworktabellenalleeninputsProperties(ComponentProperties):
        columnsSelector: List[str] = field(default_factory=list)

    def dialog(self) -> Dialog:
        return Dialog("Verwijderworktabellenalleeninputs").addElement(
            ColumnsLayout(gap="1rem", height="100%").addColumn(
                PortSchemaTabs(
                    selectedFieldsProperty=("columnsSelector")
                ).importSchema()
            )
        )

    def validate(
        self,
        context: WorkflowContext,
        component: Component[VerwijderworktabellenalleeninputsProperties],
    ) -> List[Diagnostic]:
        return []

    def onChange(
        self,
        context: WorkflowContext,
        oldState: Component[VerwijderworktabellenalleeninputsProperties],
        newState: Component[VerwijderworktabellenalleeninputsProperties],
    ) -> Component[VerwijderworktabellenalleeninputsProperties]:
        return newState

    class VerwijderworktabellenalleeninputsCode(ComponentCode):
        def __init__(self, newProps):
            self.props: Verwijderworktabellenalleeninputs.VerwijderworktabellenalleeninputsProperties = (
                newProps
            )

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            def Verwijderworktabellenalleeninputs_func(
                spark: SparkSession, params: dict
            ) -> dict:
                from pyspark.sql.functions import lit

                if "source" in params:
                    source_df: DataFrame = params["source"][0]
                else:
                    raise Exception("no source")

                result = source_df.filter(lit(False))

                return {
                    "target": [result],
                    "is_error": False,
                }

            func_params = {
                "source": [in0],
            }

            result = Verwijderworktabellenalleeninputs_func(spark, func_params)
            return result["target"][0]
