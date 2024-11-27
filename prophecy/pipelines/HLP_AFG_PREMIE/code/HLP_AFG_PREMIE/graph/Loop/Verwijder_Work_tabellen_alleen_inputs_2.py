from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Verwijder_Work_tabellen_alleen_inputs_2(
        spark: SparkSession,
        Append_ap: DataFrame, 
        BepaalPeriodeMetPrioriteit_OUTPUT0: DataFrame, 
        met_collectiviteit_rgl: DataFrame, 
        snap_rgl_pakketkorting: DataFrame, 
        Append_pk: DataFrame, 
        snap_rgl_eigenrisico: DataFrame
) -> DataFrame:
    from typing import Optional, List, Dict
    from dataclasses import dataclass, field
    from abc import ABC
    
    from pyspark.sql.column import Column
    from pyspark.sql.functions import col
    from dataclasses import dataclass
    from typing import Optional, List, Dict
    from pyspark.sql.column import Column as sparkColumn


    @dataclass(frozen = True)
    class SColumn:
        expression: Optional[Column] = None

        @staticmethod
        def getSColumn(column: str):
            return SColumn(col(column))

        def column(self) -> sparkColumn:
            return self.expression

        def columnName(self) -> str:
            return self.expression._jc.toString()


    @dataclass(frozen = True)
    class SColumnExpression:
        target: str
        expression: SColumn
        description: str
        _row_id: Optional[str] = None

        @staticmethod
        def remove_backticks(s):
            if s.startswith("`") and s.endswith("`"):
                return s[1:- 1]
            else:
                return s

        @staticmethod
        def getColumnExpression(column: str):
            return SColumnExpression(column, SColumn.getSColumn(col(column)), "")

        @staticmethod
        def getColumnsFromColumnExpressionList(columnExpressions: list):
            columnList = []

            for expression in columnExpressions:
                columnList.append(expression.expression)

            return columnList

        def column(self) -> Column:

            if (self.expression.columnName() == SColumnExpression.remove_backticks(self.target)):
                return self.expression.expression

            return self.expression.expression.alias(self.target)


    @dataclass(frozen = True)
    class VerwijderworktabellenalleeninputsProperties():
        columnsSelector: List[str] = field(default_factory = list)

    props = VerwijderworktabellenalleeninputsProperties(  #skiptraversal
        columnsSelector = []
    )
    in0 = Append_ap

    def Verwijderworktabellenalleeninputs_func(spark: SparkSession, params: dict) -> dict:
        from pyspark.sql.functions import lit

        if "source" in params:
            source_df: DataFrame = params["source"][0]
        else:
            raise Exception("no source")

        result = source_df.filter(lit(False))

        return {
"target" : [result], "is_error" : False, }

    func_params = {
"source" : [in0], }
    result = Verwijderworktabellenalleeninputs_func(spark, func_params)

    return result["target"][0]
