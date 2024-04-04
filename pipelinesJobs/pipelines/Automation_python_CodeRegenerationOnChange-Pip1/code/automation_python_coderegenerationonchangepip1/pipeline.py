from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from automation_python_coderegenerationonchangepip1.config.ConfigStore import *
from automation_python_coderegenerationonchangepip1.udfs.UDFs import *
from prophecy.utils import *
from automation_python_coderegenerationonchangepip1.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Automation_dataset2_coderegen = Automation_dataset2_coderegen(spark)
    df_Subgraph_1 = Subgraph_1(spark, Config.Subgraph_1, df_Automation_dataset2_coderegen)
    df_dataset_cust_in = dataset_cust_in(spark)
    df_Automation_dataset1_coderegen = Automation_dataset1_coderegen(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Automation_python_CodeRegenerationOnChange-Pip1")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Automation_python_CodeRegenerationOnChange-Pip1")
    registerUDFs(spark)
    
    MetricsCollector.instrument(
        spark = spark,
        pipelineId = "pipelines/Automation_python_CodeRegenerationOnChange-Pip1",
        config = Config
    )(
        pipeline
    )

if __name__ == "__main__":
    main()
