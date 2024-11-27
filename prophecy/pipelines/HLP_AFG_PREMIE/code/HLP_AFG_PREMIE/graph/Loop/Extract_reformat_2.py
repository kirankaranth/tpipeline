from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Extract_reformat_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("BOUWSTEEN_ID"), 
        col("BRN_PERSOON_ID"), 
        col("EDWH_PEIL_DTM"), 
        col("EDWH_PROCES_DTD"), 
        col("EDWH_RESOURCE_ID"), 
        col("EDWH_VERWIJDER_DTD"), 
        col("EIND_DTM"), 
        col("INGANG_DTM"), 
        col("LABEL_ID"), 
        col("LNK_PREMIE_RGL_PAKKETKORTING_ID"), 
        col("PAKKET_KORTING_PCT")
    )
