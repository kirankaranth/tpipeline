from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Join(spark: SparkSession, snap_lnk_premie: DataFrame, Extract_datum: DataFrame, ) -> DataFrame:
    return snap_lnk_premie\
        .alias("snap_lnk_premie")\
        .crossJoin(Extract_datum.alias("Extract_datum"))\
        .where(
          (
            (coalesce(col("snap_lnk_premie.INGANG_DTM"), lit("0001-01-01").cast(DateType())) <= coalesce(col("Extract_datum.EIND_DTM"), lit("0001-01-01").cast(DateType())))
            & (
              coalesce(col("snap_lnk_premie.EIND_DTM"), lit("0001-01-01").cast(DateType()))
              >= coalesce(col("Extract_datum.INGANG_DTM"), lit("0001-01-01").cast(DateType()))
            )
          )
        )\
        .select(col("snap_lnk_premie.BOUWSTEEN_ID").alias("BOUWSTEEN_ID"), col("snap_lnk_premie.COMMERCIELE_TOESLAG_BDG").alias("COMMERCIELE_TOESLAG_BDG"), col("snap_lnk_premie.EDWH_PEIL_DTM").alias("EDWH_PEIL_DTM"), col("snap_lnk_premie.EDWH_PROCES_DTD").alias("EDWH_PROCES_DTD"), col("snap_lnk_premie.EDWH_RESOURCE_ID").alias("EDWH_RESOURCE_ID"), col("snap_lnk_premie.EDWH_VERWIJDER_DTD").alias("EDWH_VERWIJDER_DTD"), col("snap_lnk_premie.EIGEN_RISICO_REGELING_ID").alias("EIGEN_RISICO_REGELING_ID"), least(col("snap_lnk_premie.EIND_DTM"), col("Extract_datum.EIND_DTM")).alias("EIND_DTM"), col("snap_lnk_premie.FABRIEKSPRIJS_BDG").alias("FABRIEKSPRIJS_BDG"), greatest(col("snap_lnk_premie.INGANG_DTM"), col("Extract_datum.INGANG_DTM")).alias("INGANG_DTM"), col("snap_lnk_premie.LEEFTIJD_TOT_MET").alias("LEEFTIJD_TOT_MET"), col("snap_lnk_premie.LEEFTIJD_VAN").alias("LEEFTIJD_VAN"), col("snap_lnk_premie.LNK_PREMIE_ID").alias("LNK_PREMIE_ID"), col("snap_lnk_premie.OVEREENKOMST_ID").alias("OVEREENKOMST_ID"), col("snap_lnk_premie.PRODUCTSET_SOORT_ID").alias("PRODUCTSET_SOORT_ID"), col("snap_lnk_premie.REFERENTIEPREMIE_BDG").alias("REFERENTIEPREMIE_BDG"), col("snap_lnk_premie.TABELPREMIE_IDC").alias("TABELPREMIE_IDC"))
