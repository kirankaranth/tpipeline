from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from automation_python_coderegenerationonchangepip2.config.ConfigStore import *
from automation_python_coderegenerationonchangepip2.udfs.UDFs import *
from prophecy.utils import *
from automation_python_coderegenerationonchangepip2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_read_customers_dataset = read_customers_dataset(spark)
    df_dataset_cust_in = dataset_cust_in(spark)
    df_reformat_with_udf = reformat_with_udf(spark, df_read_customers_dataset)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Automation_python_CodeRegenerationOnChange-Pip2")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Automation_python_CodeRegenerationOnChange-Pip2")
    registerUDFs(spark)
    
    MetricsCollector.instrument(
        spark = spark,
        pipelineId = "pipelines/Automation_python_CodeRegenerationOnChange-Pip2",
        config = Config
    )(
        pipeline
    )

if __name__ == "__main__":
    main()
