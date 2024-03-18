from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from or_hlp_peoplesoft_zorgkosten_sticky_notes.config.ConfigStore import *
from or_hlp_peoplesoft_zorgkosten_sticky_notes.udfs.UDFs import *

def Extract_aggregate(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("Kostensoort"), 
        col("BUSINESS_UNIT"), 
        col("Jaar_Maand_Verwerking"), 
        col("Jaar_Maand_Verrichting"), 
        col("ind_exact"), 
        col("ind_handmatig"), 
        col("JurEntiteit_VerzGrondsl_Code"), 
        col("ind_taxatie")
    )

    return df1.agg(
        sum(col("Bedrag_Peoplesoft")).alias("Bedrag_Peoplesoft"), 
        sum(col("Bedrag_Peoplesoft_zorgkosten")).alias("Bedrag_Peoplesoft_zorgkosten"), 
        sum(col("Bedrag_Peoplesoft_taxatie")).alias("Bedrag_Peoplesoft_taxatie")
    )
