from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Join_join_1(
        spark: SparkSession,
        snap_AFWIJKING_PREMIE: DataFrame,
        snap_AFW_PREMIE_BOUWST: DataFrame,
        snap_afw_premie_rdn: DataFrame
) -> DataFrame:
    return snap_AFWIJKING_PREMIE\
        .alias("snap_AFWIJKING_PREMIE")\
        .join(
          snap_AFW_PREMIE_BOUWST.alias("snap_AFW_PREMIE_BOUWST"),
          col("snap_AFWIJKING_PREMIE.LNK_BRN_AFWIJKING_PREMIE_ID")\
            .eqNullSafe(col("snap_AFW_PREMIE_BOUWST.LNK_BRN_AFWIJKING_PREMIE_ID")),
          "left_outer"
        )\
        .join(
          snap_afw_premie_rdn.alias("snap_afw_premie_rdn"),
          col("snap_AFWIJKING_PREMIE.AFWIJKING_PREMIE_REDEN_ID")\
            .eqNullSafe(col("snap_afw_premie_rdn.AFWIJKING_PREMIE_REDEN_ID")),
          "inner"
        )\
        .where(
          (
            coalesce(col("snap_AFWIJKING_PREMIE.EIND_DTM"), lit("0001-01-01").cast(DateType()))
            > coalesce(
              date_trunc("YEAR", (expr(Config.SYSPEILDATUM) + expr("INTERVAL '-5' YEAR"))), 
              lit("0001-01-01").cast(TimestampType())
            )
          )
        )\
        .select(col("snap_afw_premie_rdn.AFWIJKING_PREMIE_REDEN_CODE").alias("AFWIJKING_PREMIE_REDEN_CODE"), col("snap_AFW_PREMIE_BOUWST.BOUWSTEEN_ID").alias("BOUWSTEEN_ID"), col("snap_AFWIJKING_PREMIE.BRN_PERSOON_ID").alias("BRN_PERSOON_ID"), CZ_SAS_BR_T_INDICATOR(col("snap_afw_premie_rdn.AFWIJKING_PREMIE_REDEN_CODE"), lit("011"))\
        .alias("DETENTIE_IDC"), col("snap_AFWIJKING_PREMIE.EIND_DTM").alias("EIND_DTM"), col("snap_AFWIJKING_PREMIE.INGANG_DTM").alias("INGANG_DTM"), col("snap_AFWIJKING_PREMIE.OVEREENKOMST_ID").alias("OVEREENKOMST_ID"), col("snap_AFWIJKING_PREMIE.PREMIE_AFWIJKEND_BDG").alias("PREMIE_AFWIJKEND_BDG"), col("snap_AFWIJKING_PREMIE.PREMIE_AFWIJKEND_PCT").alias("PREMIE_AFWIJKEND_PCT"))
