from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def target_hlp_afg_premie(spark: SparkSession, HLP_AFG_PREMIE: DataFrame):
    HLP_AFG_PREMIE.write\
        .format("delta")\
        .mode("overwrite")\
        .saveAsTable("`sas_migration`.`HLP_AFG_PREMIE_il_hlp`.`hlp_afg_premie`")