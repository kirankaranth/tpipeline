from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from automation_python_coderegenerationonchangepip3.config.ConfigStore import *
from automation_python_coderegenerationonchangepip3.udfs.UDFs import *
from prophecy.utils import *
from automation_python_coderegenerationonchangepip3.graph import *

def pipeline(spark: SparkSession) -> None:
    df_read_customers_dataset = read_customers_dataset(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Automation_python_CodeRegenerationOnChange-Pip3")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Automation_python_CodeRegenerationOnChange-Pip3")
    registerUDFs(spark)
    
    MetricsCollector.instrument(
        spark = spark,
        pipelineId = "pipelines/Automation_python_CodeRegenerationOnChange-Pip3",
        config = Config
    )(
        pipeline
    )

if __name__ == "__main__":
    main()
