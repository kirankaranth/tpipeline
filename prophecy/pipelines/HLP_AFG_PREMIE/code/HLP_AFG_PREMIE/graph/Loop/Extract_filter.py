from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Extract_filter(spark: SparkSession, DATUM: DataFrame) -> DataFrame:
    return DATUM.filter(
        (
          (coalesce(col("JAAR_NMR"), lit("-inf").cast(FloatType())) >= lit(2000))
          & (
            coalesce(col("JAAR_NMR"), lit("-inf").cast(FloatType()))
            <= coalesce((year(expr(Config.SYSPEILDATUM)) + lit(1)), lit("-inf").cast(FloatType()))
          )
        )
    )
