from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from or_hlp_peoplesoft_zorgkosten_sticky_notes.config.ConfigStore import *
from or_hlp_peoplesoft_zorgkosten_sticky_notes.udfs.UDFs import *

def Sticky_Note_2(spark: SparkSession):
    # HST_tabellen bevatten geen EDWH-historie

    return 