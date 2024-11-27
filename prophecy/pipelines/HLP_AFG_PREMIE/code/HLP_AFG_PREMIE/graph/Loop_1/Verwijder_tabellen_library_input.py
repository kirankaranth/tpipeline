from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Verwijder_tabellen_library_input(spark: SparkSession, TMP_HLP_AFG_PREMIE_BLOK: DataFrame):
    # Original transform id: A5H5U7VN.BX005AR2
    # Original transform name: Verwijder tabellen library input
    # Original transform description: Verwijder tabellen library input
    #
    # Auto-generated input/output variables.
    # Variables for input ports
    TMP_HLP_AFG_PREMIE_BLOK.createOrReplaceTempView('_INPUT')
    _input = '_INPUT'
    TMP_HLP_AFG_PREMIE_BLOK.createOrReplaceTempView('_INPUT1')
    _input1 = '_INPUT1'
    verwijdertabellenlibraryinput()

    return 
