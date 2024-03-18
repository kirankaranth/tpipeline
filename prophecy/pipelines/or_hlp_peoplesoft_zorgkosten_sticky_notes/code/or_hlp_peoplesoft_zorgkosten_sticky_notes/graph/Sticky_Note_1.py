from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from or_hlp_peoplesoft_zorgkosten_sticky_notes.config.ConfigStore import *
from or_hlp_peoplesoft_zorgkosten_sticky_notes.udfs.UDFs import *

def Sticky_Note_1(spark: SparkSession):
    # Indien DESCR254 begint met AP of AR, dan betreft het MACRO boekingen welke met de introductie van Force handmatig in PeopleSoft worden geboekt. Deze worden dus hier als handmatig gezien omdat dit niet als onderdeel van ind_handmatig wordt meegenomen.

    return 
