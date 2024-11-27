from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Join_orderby(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(
        col("BOUWSTEEN_ID").asc(), 
        col("BRN_PERSOON_ID").asc(), 
        col("COLLECTIVITEIT_OVEREENKOMST_ID").asc(), 
        col("INGANG_DTM").asc(), 
        col("LABEL_ID").asc(), 
        col("LAND_ID").asc(), 
        col("PRODUCTSET_SOORT_ID").asc()
    )
