from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Extract_filter_1(spark: SparkSession, HLP_PREMIE_PARAMETERS: DataFrame) -> DataFrame:
    return HLP_PREMIE_PARAMETERS.filter(
        (
          (col("BRN_PERSOON_ID") > lit(Config.EERSTE_BRN_PERSOON_ID))
          & (col("BRN_PERSOON_ID") <= lit(Config.LAATSTE_BRN_PERSOON_ID))
        )
    )
