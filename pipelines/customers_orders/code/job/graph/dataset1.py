from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def dataset1(spark: SparkSession) -> DataFrame:
    return spark.read.format("avro").load("dsafdsf")
