from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Extract_aggregate(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("JAAR_NMR"))

    return df1.agg(max(col("SAS_DTM")).alias("EIND_DTM"), min(col("SAS_DTM")).alias("INGANG_DTM"))
