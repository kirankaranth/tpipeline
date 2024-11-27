from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from HLP_AFG_PREMIE.config.ConfigStore import *
from HLP_AFG_PREMIE.udfs.UDFs import *
from prophecy.utils import *
from HLP_AFG_PREMIE.graph import *

def pipeline(spark: SparkSession) -> None:
    df_source_hlp_premie_parameters = source_hlp_premie_parameters(spark)
    df_User_Written = User_Written(spark, df_source_hlp_premie_parameters)
    Loop(Config.Loop).apply(spark, df_User_Written)
    Loop_1(Config.Loop_1).apply(spark, df_User_Written)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/HLP_AFG_PREMIE")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/HLP_AFG_PREMIE", config = Config)(pipeline)

if __name__ == "__main__":
    main()
