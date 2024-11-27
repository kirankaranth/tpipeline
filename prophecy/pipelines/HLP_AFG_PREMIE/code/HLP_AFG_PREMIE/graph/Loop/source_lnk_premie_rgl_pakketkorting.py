from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def source_lnk_premie_rgl_pakketkorting(spark: SparkSession) -> DataFrame:
    return spark.read.table("`sas_migration`.`HLP_AFG_PREMIE_il_edwh`.`lnk_premie_rgl_pakketkorting`")
