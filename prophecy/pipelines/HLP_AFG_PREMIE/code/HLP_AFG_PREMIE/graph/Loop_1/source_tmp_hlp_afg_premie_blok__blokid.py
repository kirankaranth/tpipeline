from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def source_tmp_hlp_afg_premie_blok__blokid(spark: SparkSession) -> DataFrame:
    return spark.read.table("`sas_migration`.`HLP_AFG_PREMIE_il_hlp`.`tmp_hlp_afg_premie_blok_&blokid.`")
