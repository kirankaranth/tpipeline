from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Join_dedup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.distinct()