from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def User_Written_2(spark: SparkSession) -> DataFrame:
    # Original transform id: A5H5U7VN.BX005AR0
    # Original transform name: User Written
    # Original transform description: %LET blokid = &NUMMER;
    #
    # Auto-generated input/output variables.
    # Variables for output ports
    _output = '_OUTPUT'
    blokid = nummer
    # Auto-generated variables.
    # Variables for output ports
    User_Written = spark.table("_OUTPUT")

    return User_Written
