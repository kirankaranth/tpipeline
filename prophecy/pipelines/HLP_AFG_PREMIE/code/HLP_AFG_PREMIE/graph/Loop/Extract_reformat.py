from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Extract_reformat(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("BETAAL_TERMIJN_ID"), 
        col("BOUWSTEEN_ID"), 
        col("BRN_PERSOON_ID"), 
        col("COLLECTIVITEIT_OVEREENKOMST_ID"), 
        col("EIGEN_RISICO_REGELING_ID"), 
        col("EIND_DTM"), 
        col("GEBOORTE_DTM"), 
        col("GESLACHT_ID"), 
        col("INGANG_DTM"), 
        col("LABEL_ID"), 
        col("LAND_ID"), 
        col("PRODUCTSET_SOORT_ID"), 
        col("ZORGVERZEKERING_IDC"), 
        col("ZORGVERZ_OVEREENKOMST_ID")
    )
