from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from automation_python_coderegenerationonchangepip1.udfs.UDFs import *
from . import *
from .config import *

def Subgraph_1(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_Reformat_1 = Reformat_1(spark, in0)
    subgraph_config.update(Config)

    return df_Reformat_1
