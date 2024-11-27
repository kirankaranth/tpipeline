from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Extract_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("BETAAL_TERMIJN_ID"), 
        col("BOUWSTEEN_ID"), 
        col("BRN_PERSOON_ID"), 
        col("COLLECTIVITEIT_OVEREENKOMST_ID"), 
        col("COMMERCIELE_TOESLAG_BDG"), 
        col("EDWH_RESOURCE_ID"), 
        col("EIGEN_RISICO_REGELING_ID"), 
        when(
            (
              coalesce(col("EIND_DTM"), lit("0001-01-01").cast(DateType()))
              > coalesce(
                (
                  date_trunc("YEAR", (expr(Config.JOB_CURRENT_DATE) + expr("INTERVAL '2' YEAR")))
                  - expr("make_dt_interval(0, 0, 0, 1)")
                ), 
                lit("0001-01-01").cast(TimestampType())
              )
            ), 
            (
              date_trunc("YEAR", (expr(Config.JOB_CURRENT_DATE) + expr("INTERVAL '2' YEAR")))
              - expr("make_dt_interval(0, 0, 0, 1)")
            )
          )\
          .otherwise(col("EIND_DTM"))\
          .cast(DateType())\
          .alias("EIND_DTM"), 
        col("FABRIEKSPRIJS_BDG"), 
        col("GEBOORTE_DTM"), 
        col("GESLACHT_ID"), 
        when(
            (
              coalesce(col("INGANG_DTM"), lit("0001-01-01").cast(DateType()))
              < coalesce(
                date_trunc("YEAR", (expr(Config.JOB_CURRENT_DATE) + expr("INTERVAL '-5' YEAR"))), 
                lit("0001-01-01").cast(TimestampType())
              )
            ), 
            date_trunc("YEAR", (expr(Config.JOB_CURRENT_DATE) + expr("INTERVAL '-5' YEAR")))
          )\
          .otherwise(col("INGANG_DTM"))\
          .cast(DateType())\
          .alias("INGANG_DTM"), 
        col("LABEL_ID"), 
        col("LABEL_KEY"), 
        col("LAND_ID"), 
        col("LEEFTIJD_TOT_MET"), 
        col("LEEFTIJD_VAN"), 
        col("PRODUCTSET_SOORT_ID"), 
        col("REFERENTIEPREMIE_BDG"), 
        col("REKEN_GEBOORTE_DTM"), 
        col("TABELPREMIE_IDC"), 
        col("ZORGVERZEKERING_IDC"), 
        col("ZORGVERZ_OVEREENKOMST_ID")
    )
