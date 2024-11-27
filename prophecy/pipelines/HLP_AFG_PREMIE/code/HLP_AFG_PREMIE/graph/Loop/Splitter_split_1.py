from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Splitter_split_1(spark: SparkSession, afw_premie: DataFrame) -> (DataFrame):

    try:
        registerUDFs(spark)
    except NameError:
        print("registerUDFs not working")

    afw_premie.createOrReplaceTempView("afw_premie")
    df1 = spark.sql(
        "SELECT\n  BRN_PERSOON_ID,\n  OVEREENKOMST_ID,\n  PREMIE_AFWIJKEND_BDG,\n  PREMIE_AFWIJKEND_PCT,\n  INGANG_DTM,\n  EIND_DTM,\n  BOUWSTEEN_ID,\n  DETENTIE_IDC,\n  AFWIJKING_PREMIE_REDEN_CODE\nFROM afw_premie\nWHERE\n  BOUWSTEEN_ID IS NULL"
    )

    return df1
