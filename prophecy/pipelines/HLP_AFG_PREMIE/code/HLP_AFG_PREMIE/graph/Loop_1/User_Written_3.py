from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def User_Written_3(spark: SparkSession, TMP_HLP_AFG_PREMIE_BLOK: DataFrame) -> DataFrame:
    # Original transform id: A5H5U7VN.BX005AR1
    # Original transform name: User Written
    # Original transform description: Appenden TMP
    #
    # Auto-generated input/output variables.
    # Variables for input ports
    TMP_HLP_AFG_PREMIE_BLOK.createOrReplaceTempView('_INPUT')
    _input = '_INPUT'
    TMP_HLP_AFG_PREMIE_BLOK.createOrReplaceTempView('_INPUT1')
    _input1 = '_INPUT1'
    # Variables for output ports
    _output = '_OUTPUT'
    # Original code:
    #
    # %MACRO VULLEN_TEMP;
    #     /* Bij de eerste nieuw aanmaken */
    #     %IF &BLOKID = 1 %THEN %DO;
    #        data &_OUTPUT.(%unquote(&_OUTPUT_options));
    #           set &_INPUT;
    #        run;
    #     %END;
    #     /* Anders appenden */
    #     %ELSE %DO;
    #        proc append base=&_OUTPUT.(%unquote(&_OUTPUT_options)) data=&_INPUT;
    #        run;
    #     %END;
    # %MEND;
    #
    # %VULLEN_TEMP;
    #
    # Auto-generated variables.
    # Variables for output ports
    HLP_AFG_PREMIE = spark.table("_OUTPUT")

    return HLP_AFG_PREMIE
