from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Append_subset_cols(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("BETAAL_TERMIJN_ID"), 
        col("BOUWSTEEN_ID"), 
        col("BRN_PERSOON_ID"), 
        col("COLLECTIVITEIT_OVEREENKOMST_ID"), 
        col("COMMERCIELE_TOESLAG_BDG"), 
        col("DETENTIE_IDC"), 
        col("EDWH_RESOURCE_ID"), 
        col("EIGEN_RISICO_REGELING_ID"), 
        col("EIND_DTM"), 
        col("FABRIEKSPRIJS_BDG"), 
        col("GEBOORTE_DTM"), 
        col("GESLACHT_ID"), 
        col("INGANG_DTM"), 
        col("LABEL_ID"), 
        col("LAND_ID"), 
        col("LEEFTIJD_BDG"), 
        col("LEEFTIJD_PCT"), 
        col("LEEFTIJD_TOT_MET"), 
        col("LEEFTIJD_VAN"), 
        col("LFT_COMM_TOESLAG_BDG"), 
        col("LFT_FABRIEKSPRIJS_BDG"), 
        col("PREMIE_AFWIJKEND_BDG"), 
        col("PREMIE_AFWIJKEND_PCT"), 
        col("PRIORITEIT"), 
        col("PRODUCTSET_SOORT_ID"), 
        col("REFERENTIEPREMIE_BDG"), 
        col("TABELPREMIE_IDC"), 
        col("ZORGVERZEKERING_IDC"), 
        col("ZORGVERZ_OVEREENKOMST_ID")
    )
