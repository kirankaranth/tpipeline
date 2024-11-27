from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Extract_filter_2(spark: SparkSession, Join_pers_premie: DataFrame) -> DataFrame:
    return Join_pers_premie.filter(
        (
          (coalesce(col("EIND_DTM"), lit("0001-01-01").cast(DateType())) > coalesce(date_trunc("YEAR", (expr(Config.SYSPEILDATUM) + expr("INTERVAL '-5' YEAR"))), lit("0001-01-01").cast(TimestampType())))
          & (
            coalesce(col("INGANG_DTM"), lit("0001-01-01").cast(DateType()))
            < coalesce(
              (
                date_trunc("YEAR", (expr(Config.SYSPEILDATUM) + expr("INTERVAL '2' YEAR")))
                - expr("make_dt_interval(0, 0, 0, 1)")
              ), 
              lit("0001-01-01").cast(TimestampType())
            )
          )
        )
    )
