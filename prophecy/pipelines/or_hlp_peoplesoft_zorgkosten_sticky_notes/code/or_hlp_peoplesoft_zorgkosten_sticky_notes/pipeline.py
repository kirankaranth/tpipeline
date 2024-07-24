from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from or_hlp_peoplesoft_zorgkosten_sticky_notes.config.ConfigStore import *
from or_hlp_peoplesoft_zorgkosten_sticky_notes.udfs.UDFs import *
from prophecy.utils import *
from or_hlp_peoplesoft_zorgkosten_sticky_notes.graph import *

def pipeline(spark: SparkSession) -> None:
    Sticky_Note_1(spark)
    Sticky_Note_2(spark)
    df_source_stg_pst_ps_jrnl_ln_hst = source_stg_pst_ps_jrnl_ln_hst(spark)
    df_source_stg_pst_ps_jrnl_header_hst = source_stg_pst_ps_jrnl_header_hst(spark)
    df_Join = Join(spark, df_source_stg_pst_ps_jrnl_ln_hst, df_source_stg_pst_ps_jrnl_header_hst)
    df_Extract_aggregate = Extract_aggregate(spark, df_Join)
    df_Table_Loader = Table_Loader(spark, df_Extract_aggregate)
    target_or_hlp_peoplesoft_zorgkosten(spark, df_Table_Loader)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/or_hlp_peoplesoft_zorgkosten_sticky_notes")
    registerUDFs(spark)
    
    MetricsCollector.instrument(
        spark = spark,
        pipelineId = "pipelines/or_hlp_peoplesoft_zorgkosten_sticky_notes",
        config = Config
    )(
        pipeline
    )

if __name__ == "__main__":
    main()
