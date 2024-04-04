from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from automation_python_coderegenerationonchangepip2.config.ConfigStore import *
from automation_python_coderegenerationonchangepip2.udfs.UDFs import *

def reformat_with_udf(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(udfSingleIntInput(lit(2)).alias("c_with_udf"))
