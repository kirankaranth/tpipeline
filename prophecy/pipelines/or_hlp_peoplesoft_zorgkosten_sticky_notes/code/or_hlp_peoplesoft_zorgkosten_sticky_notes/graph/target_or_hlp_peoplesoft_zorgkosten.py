from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from or_hlp_peoplesoft_zorgkosten_sticky_notes.config.ConfigStore import *
from or_hlp_peoplesoft_zorgkosten_sticky_notes.udfs.UDFs import *

def target_or_hlp_peoplesoft_zorgkosten(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("error").saveAsTable("`bm_20230428_2_or_hlp`.`or_hlp_peoplesoft_zorgkosten`")
