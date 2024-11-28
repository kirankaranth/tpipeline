from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def BepaalPeriodeMetPrioriteit_1(spark: SparkSession, Append_pk: DataFrame) -> DataFrame:
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
    class BepaalPeriodeMetPrioriteitProperties():
        columnsSelector: List[str] = field(default_factory = list)
        SleutelWaarden: str = str("")
        AttribuutWaarden: str = str("")
        IngangsDatum: str = str("")
        EindDatum: str = str("")
        Prioriteit: str = str("")

    props = BepaalPeriodeMetPrioriteitProperties(  #skiptraversal
        columnsSelector = [], 
        SleutelWaarden = "BRN_PERSOON_ID,ZORGVERZ_OVEREENKOMST_ID,BOUWSTEEN_ID,LABEL_ID,COLLECTIVITEIT_OVEREENKOMST_ID", 
        AttribuutWaarden = "Zorgverzekering_IDC,GEBOORTE_DTM,GESLACHT_ID,LAND_ID,PRODUCTSET_SOORT_ID,BETAAL_TERMIJN_ID,EIGEN_RISICO_REGELING_ID,REFERENTIEPREMIE_BDG,FABRIEKSPRIJS_BDG,COMMERCIELE_TOESLAG_BDG,EDWH_RESOURCE_ID,LEEFTIJD_VAN,LEEFTIJD_TOT_MET,LEEFTIJD_BDG,LEEFTIJD_PCT,LFT_FABRIEKSPRIJS_BDG,LFT_COMM_TOESLAG_BDG,PREMIE_AFWIJKEND_BDG,PREMIE_AFWIJKEND_PCT,COLLECTIVITEIT_BDG,COLLECTIVITEIT_PCT,PAKKET_KORTING_PCT,DETENTIE_IDC,TABELPREMIE_IDC", 
        IngangsDatum = "INGANG_DTM", 
        EindDatum = "EIND_DTM", 
        Prioriteit = "PRIORITEIT"
    )
    in0 = Append_pk

    def bepaalperiodemetprioriteit(spark: SparkSession, params: dict) -> dict:
        result = {}

        if "source" in params:
            source_df: DataFrame = params["source"][0]
        else:
            raise Exception("no source")

        if "SleutelWaarden" in params:
            SleutelWaarden = params["SleutelWaarden"]
            SleutelWaarden_lst = SleutelWaarden.split(",")
        else:
            raise Exception("no SleutelWaarden")

        if "AttribuutWaarden" in params:
            AttribuutWaarden = params["AttribuutWaarden"]
            AttribuutWaarden_lst = AttribuutWaarden.split(",")
        else:
            raise Exception("no AttribuutWaarden")

        if "IngangsDatum" in params:
            IngangsDatum = params["IngangsDatum"]
        else:
            raise Exception("no IngangsDatum")

        if "EindDatum" in params:
            EindDatum = params["EindDatum"]
        else:
            raise Exception("no EindDatum")

        if "Prioriteit" in params:
            Prioriteit = params["Prioriteit"]
        else:
            raise Exception("no Prioriteit")

        source_df.createOrReplaceTempView("source")
        datums = spark.sql(
            f"""
                        SELECT {SleutelWaarden},
                            {IngangsDatum} AS relevante_dtm,
                            CAST(1 AS DOUBLE) AS DUMMY
                        FROM source
                        UNION ALL
                        SELECT {SleutelWaarden},
                            {EindDatum} AS relevante_dtm,
                            CAST(1 AS DOUBLE) AS DUMMY
                        FROM source"""
        )
        datums.createOrReplaceTempView("datums")
        datums_sort = spark.sql(f"""SELECT DISTINCT {SleutelWaarden}, relevante_dtm from datums""")
        datums_sort.createOrReplaceTempView("datums_sort")
        periode = spark.sql(
            f"""
                        WITH cte2 AS (
                                SELECT {SleutelWaarden}
                                    ,relevante_dtm
                                    ,CAST(1 AS DOUBLE) AS DUMMY
                                    ,ROW_NUMBER() OVER (
                                        PARTITION BY {SleutelWaarden} ORDER BY relevante_dtm
                                        ) AS rn
                                    ,ROW_NUMBER() OVER (
                                        PARTITION BY {SleutelWaarden} ORDER BY relevante_dtm DESC
                                        ) AS rn_l
                                    ,LEAD(relevante_dtm) OVER (
                                        PARTITION BY {SleutelWaarden} ORDER BY relevante_dtm
                                        ) AS lead_relevante_dtm
                                    ,relevante_dtm
                                FROM datums_sort
                                )
                            SELECT {SleutelWaarden}
                                ,NULL AS ingang_relevante_dtm
                                ,relevante_dtm AS eind_relevante_dtm
                                ,DUMMY
                                ,relevante_dtm
                            FROM cte2
                            WHERE rn = 1
                            UNION ALL
                            SELECT {SleutelWaarden}
                                ,relevante_dtm AS ingang_relevante_dtm
                                ,lead_relevante_dtm AS eind_relevante_dtm
                                ,DUMMY
                                ,relevante_dtm
                            FROM cte2
                            WHERE rn = 1
                                AND lead_relevante_dtm IS NOT NULL
                            UNION ALL
                            SELECT {SleutelWaarden}
                                ,relevante_dtm AS ingang_relevante_dtm
                                ,lead_relevante_dtm AS eind_relevante_dtm
                                ,DUMMY
                                ,relevante_dtm
                            FROM cte2
                            WHERE rn != 1 AND rn_l != 1
                                """
        )
        periode.createOrReplaceTempView("periode")
        conditions = []
        predicate = ""

        # Iterate through each element to construct the conditions
        for element in SleutelWaarden_lst:
            condition = f"COALESCE(t1.{element},'') = COALESCE(t2.{element},'')"
            conditions.append(condition)

        # Join the conditions with 'and' to form the final string
        predicate = " and ".join(conditions)
        SleutelWaarden_lst_t1 = []
        SleutelWaarden_lst_t2 = []

        for i in range(len(SleutelWaarden_lst)):
            SleutelWaarden_lst_t1.append("t1." + SleutelWaarden_lst[i])
            SleutelWaarden_lst_t2.append("t2." + SleutelWaarden_lst[i])

        SleutelWaarden_t1 = ",".join(SleutelWaarden_lst_t1)
        SleutelWaarden_t2 = ",".join(SleutelWaarden_lst_t2)
        AttribuutWaarden_lst_t2 = []

        for i in range(len(AttribuutWaarden_lst)):
            AttribuutWaarden_lst_t2.append("t2." + AttribuutWaarden_lst[i])

        AttribuutWaarden_t2 = ",".join(AttribuutWaarden_lst_t2)
        AttribuutWaarden_t2 = ",".join(AttribuutWaarden_lst_t2)
        join_periode_in1 = spark.sql(
            f"""
                                SELECT {SleutelWaarden_t1},
                                    t1.{IngangsDatum},
                                    t1.{EindDatum},
                                    CASE
                                        WHEN t1.{IngangsDatum} = t1.{EindDatum} THEN t1.{IngangsDatum}
                                        ELSE COALESCE(t2.ingang_relevante_dtm, t1.{IngangsDatum})
                                    END AS ingang_relevante_dtm,
                                    CASE
                                        WHEN t1.{IngangsDatum} = t1.{EindDatum} THEN t1.{IngangsDatum}
                                        ELSE COALESCE(t2.eind_relevante_dtm, t1.{EindDatum})
                                    END AS eind_relevante_dtm,
                                    t1.{Prioriteit},
                                    {AttribuutWaarden},
                                    CASE
                                        WHEN t1.{IngangsDatum} != t2.ingang_relevante_dtm AND t1.{EindDatum} != t2.eind_relevante_dtm AND datediff(t2.eind_relevante_dtm,t2.ingang_relevante_dtm) = 1 THEN 'filter' ELSE NULL
                                    END AS Filterset
                                FROM source t1
                                JOIN periode t2 ON {predicate}
                                where  (t1.{IngangsDatum} <= t2.ingang_relevante_dtm AND t1.{EindDatum} >= t2.eind_relevante_dtm)
                                OR (t1.{IngangsDatum} = t2.ingang_relevante_dtm AND t1.{EindDatum} = t2.ingang_relevante_dtm)
                                OR (t1.{IngangsDatum} = t1.{EindDatum} AND t1.{EindDatum} = t2.eind_relevante_dtm)
                                """
        )
        join_periode_in1.createOrReplaceTempView("join_periode_in1")
        join_periode_in2 = spark.sql(
            f"""
                        SELECT {SleutelWaarden_t1}
                            ,t1.ingang_relevante_dtm
                            ,CASE 
                                WHEN t1.{IngangsDatum} = t1.{EindDatum}
                                    THEN t1.ingang_relevante_dtm
                                ELSE t1.eind_relevante_dtm
                                END AS eind_relevante_dtm
                            ,MIN(t1.{Prioriteit}) OVER (
                                PARTITION BY {SleutelWaarden}
                                ,ingang_relevante_dtm
                                ,eind_relevante_dtm
                                ) AS {Prioriteit}
                        FROM join_periode_in1 t1
                        WHERE t1.filterset IS NULL
                                """
        )
        join_periode_in2.createOrReplaceTempView("join_periode_in2")
        join_periode_in1_col = [
                                   element.upper()
                                   for element in join_periode_in1.columns
                                   ]
        join_periode_in2_col = [
                                   element.upper()
                                   for element in join_periode_in2.columns
                                   ]
        intersection = [
                           value
                           for value in join_periode_in1_col
                           if value in join_periode_in2_col
                           ]
        conditions = []
        predicate = ""

        # Iterate through each element to construct the conditions
        for element in intersection:
            condition = f"COALESCE(t1.{element},'') = COALESCE(t2.{element},'')"
            conditions.append(condition)

        # Join the conditions with 'and' to form the final string
        predicate = " and ".join(conditions)
        voorbereiden_op = spark.sql(
            f"""
                            SELECT DISTINCT {SleutelWaarden_t2}
                                ,t1.ingang_relevante_dtm
                                ,t1.eind_relevante_dtm
                                ,t2.{Prioriteit}
                                ,{AttribuutWaarden_t2}
                                ,CAST(1 AS DOUBLE) AS DUMMY
                            FROM join_periode_in2 t1
                            JOIN join_periode_in1 t2 ON {predicate}
                                """
        )
        voorbereiden_op.createOrReplaceTempView("voorbereiden_op")
        aansluiten_op = spark.sql(
            f"""
                        WITH cte AS (
                            SELECT 
                                *
                                ,row_number() OVER (
                                    PARTITION BY {SleutelWaarden} ORDER BY ingang_relevante_dtm, eind_relevante_dtm
                                    ) AS rn
                                ,LAG(ingang_relevante_dtm, 1, to_date('1960-01-01')) OVER (
                                    PARTITION BY {SleutelWaarden} ORDER BY ingang_relevante_dtm, eind_relevante_dtm
                                    ) AS prev_ingang
                                ,LAG(eind_relevante_dtm, 1, to_date('1960-01-01')) OVER (
                                    PARTITION BY {SleutelWaarden} ORDER BY ingang_relevante_dtm, eind_relevante_dtm
                                    ) AS prev_eind
                                ,LAG(PRIORITEIT, 1, 0) OVER (
                                    PARTITION BY {SleutelWaarden} ORDER BY ingang_relevante_dtm, eind_relevante_dtm
                                    ) AS prev_prio
                            FROM voorbereiden_op
                            )
                        SELECT 
                            {SleutelWaarden}
                            ,CASE 
                                WHEN ingang_relevante_dtm = prev_eind
                                    AND {Prioriteit} >= prev_prio
                                    THEN ingang_relevante_dtm + 1
                                ELSE ingang_relevante_dtm
                            END AS ingang_relevante_dtm
                            ,eind_relevante_dtm
                            ,{Prioriteit}
                            ,{AttribuutWaarden}
                            ,DUMMY
                        FROM cte
                        """
        )
        aansluiten_op.createOrReplaceTempView("aansluiten_op")
        target_df = spark.sql(
            f"""
                        WITH cte AS (
                            SELECT 
                                *
                                ,row_number() OVER (
                                    PARTITION BY {SleutelWaarden} ORDER BY ingang_relevante_dtm DESC, eind_relevante_dtm 
                                ) AS rn
                                ,LAG(ingang_relevante_dtm) OVER (
                                    PARTITION BY {SleutelWaarden} ORDER BY ingang_relevante_dtm DESC, eind_relevante_dtm 
                                    ) AS prev_ingang
                                ,LAG(eind_relevante_dtm) OVER (
                                    PARTITION BY {SleutelWaarden} ORDER BY ingang_relevante_dtm DESC, eind_relevante_dtm 
                                    ) AS prev_eind
                                ,LAG(PRIORITEIT) OVER (
                                    PARTITION BY {SleutelWaarden} ORDER BY ingang_relevante_dtm DESC, eind_relevante_dtm 
                                    ) AS prev_prio
                            FROM aansluiten_op
                            )                        
                        SELECT DISTINCT
                            {SleutelWaarden}
                            ,ingang_relevante_dtm
                            ,CASE 
                                WHEN eind_relevante_dtm = prev_ingang
                                    AND {Prioriteit} >= prev_prio
                                    THEN eind_relevante_dtm - 1
                                ELSE eind_relevante_dtm
                            END AS eind_relevante_dtm
                            ,{Prioriteit}
                            ,{AttribuutWaarden}
                            ,ingang_relevante_dtm AS prev_ingang
                            ,CASE 
                                WHEN eind_relevante_dtm = prev_ingang
                                    AND {Prioriteit} >= prev_prio
                                    THEN eind_relevante_dtm - 1
                                ELSE eind_relevante_dtm
                            END AS prev_eind
                            ,{Prioriteit} AS prev_prio
                            ,DUMMY
                        FROM cte
                """
        )
        result["target"] = [target_df]
        result["is_error"] = False

        return result

    params = {
        "source": [in0],
        "SleutelWaarden": props.SleutelWaarden,
        "AttribuutWaarden": props.AttribuutWaarden,
        "IngangsDatum": props.IngangsDatum,
        "EindDatum": props.EindDatum,
        "Prioriteit": props.Prioriteit,
    }

    return bepaalperiodemetprioriteit(spark, params)["target"][0]