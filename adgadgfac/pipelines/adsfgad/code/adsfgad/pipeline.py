from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from adsfgad.config.ConfigStore import *
from adsfgad.udfs.UDFs import *
from prophecy.utils import *
from adsfgad.graph import *

def pipeline(spark: SparkSession) -> None:
    Target_0(spark)
    Target_1(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/adsfgad")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/adsfgad")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
