from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Extract_filter_3(spark: SparkSession, snap_rgl_pakketkorting: DataFrame) -> DataFrame:
    return snap_rgl_pakketkorting.filter(
        (
          coalesce(col("EIND_DTM"), lit("0001-01-01").cast(DateType()))
          > coalesce(
            date_trunc("YEAR", (expr(Config.SYSPEILDATUM) + expr("INTERVAL '-5' YEAR"))), 
            lit("0001-01-01").cast(TimestampType())
          )
        )
    )
