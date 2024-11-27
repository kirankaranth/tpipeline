from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Join_6(spark: SparkSession, met_buitenland: DataFrame, snap_eigenprod: DataFrame, ) -> DataFrame:
    return met_buitenland\
        .alias("met_buitenland")\
        .join(
          snap_eigenprod.alias("snap_eigenprod"),
          (
            ((col("met_buitenland.COLLECTIVITEIT_OVEREENKOMST_ID").eqNullSafe(col("snap_eigenprod.COLLECTIVITEIT_OVEREENKOMST_ID")) & col("met_buitenland.PRODUCTSET_SOORT_ID").eqNullSafe(col("snap_eigenprod.PRODUCTSET_SOORT_ID"))) & col("met_buitenland.BOUWSTEEN_ID").eqNullSafe(col("snap_eigenprod.BOUWSTEEN_ID")))
            & (
              (coalesce(col("met_buitenland.BL_INGANG_DTM"), lit("0001-01-01").cast(DateType())) <= coalesce(col("snap_eigenprod.EIND_DTM"), lit("0001-01-01").cast(DateType())))
              & (
                coalesce(col("met_buitenland.BL_EIND_DTM"), lit("0001-01-01").cast(DateType()))
                >= coalesce(col("snap_eigenprod.INGANG_DTM"), lit("0001-01-01").cast(DateType()))
              )
            )
          ),
          "left_outer"
        )\
        .select(col("met_buitenland.ASSURANTIE_PCT").alias("ASSURANTIE_PCT"), col("met_buitenland.BETAAL_TERMIJN_ID").alias("BETAAL_TERMIJN_ID"), col("met_buitenland.BOUWSTEEN_ID").alias("BOUWSTEEN_ID"), col("met_buitenland.BRN_PERSOON_ID").alias("BRN_PERSOON_ID"), col("met_buitenland.BUITENLAND_BDG").alias("BUITENLAND_BDG"), col("met_buitenland.BUITENLAND_PCT").alias("BUITENLAND_PCT"), col("met_buitenland.COLLECTIVITEIT_BDG").alias("COLLECTIVITEIT_BDG"), col("met_buitenland.COLLECTIVITEIT_OVEREENKOMST_ID").alias("COLLECTIVITEIT_OVEREENKOMST_ID"), col("met_buitenland.COLLECTIVITEIT_PCT").alias("COLLECTIVITEIT_PCT"), col("met_buitenland.COMMERCIELE_TOESLAG_BDG").alias("COMMERCIELE_TOESLAG_BDG"), col("met_buitenland.DETENTIE_IDC").alias("DETENTIE_IDC"), col("met_buitenland.EDWH_RESOURCE_ID").alias("EDWH_RESOURCE_ID"), col("snap_eigenprod.EIGEN_PRODUCT_BDG").alias("EIGEN_PRODUCT_BDG"), col("snap_eigenprod.EIGEN_PRODUCT_PCT").alias("EIGEN_PRODUCT_PCT"), col("met_buitenland.EIGEN_RISICO_BDG").alias("EIGEN_RISICO_BDG"), col("met_buitenland.EIGEN_RISICO_PCT").alias("EIGEN_RISICO_PCT"), col("met_buitenland.EIGEN_RISICO_REGELING_ID").alias("EIGEN_RISICO_REGELING_ID"), least(col("met_buitenland.BL_EIND_DTM"), col("snap_eigenprod.EIND_DTM")).alias("EIND_DTM"), col("met_buitenland.ER_COMM_TOESLAG_BDG").alias("ER_COMM_TOESLAG_BDG"), col("met_buitenland.ER_FABRIEKSPRIJS_BDG").alias("ER_FABRIEKSPRIJS_BDG"), col("met_buitenland.FABRIEKSPRIJS_BDG").alias("FABRIEKSPRIJS_BDG"), col("met_buitenland.GEBOORTE_DTM").alias("GEBOORTE_DTM"), col("met_buitenland.GESLACHT_ID").alias("GESLACHT_ID"), greatest(col("met_buitenland.BL_INGANG_DTM"), col("snap_eigenprod.INGANG_DTM")).alias("INGANG_DTM"), col("met_buitenland.LABEL_ID").alias("LABEL_ID"), col("met_buitenland.LAND_ID").alias("LAND_ID"), col("met_buitenland.LEEFTIJD_BDG").alias("LEEFTIJD_BDG"), col("met_buitenland.LEEFTIJD_PCT").alias("LEEFTIJD_PCT"), col("met_buitenland.LEEFTIJD_TOT_MET").alias("LEEFTIJD_TOT_MET"), col("met_buitenland.LEEFTIJD_TOT_MET_BL").alias("LEEFTIJD_TOT_MET_BL"), col("met_buitenland.LEEFTIJD_TOT_MET_ER").alias("LEEFTIJD_TOT_MET_ER"), col("met_buitenland.LEEFTIJD_VAN").alias("LEEFTIJD_VAN"), col("met_buitenland.LEEFTIJD_VAN_BL").alias("LEEFTIJD_VAN_BL"), col("met_buitenland.LEEFTIJD_VAN_ER").alias("LEEFTIJD_VAN_ER"), col("met_buitenland.LFT_COMM_TOESLAG_BDG").alias("LFT_COMM_TOESLAG_BDG"), col("met_buitenland.LFT_FABRIEKSPRIJS_BDG").alias("LFT_FABRIEKSPRIJS_BDG"), col("met_buitenland.PAKKET_KORTING_PCT").alias("PAKKET_KORTING_PCT"), col("met_buitenland.PREMIE_AFWIJKEND_BDG").alias("PREMIE_AFWIJKEND_BDG"), col("met_buitenland.PREMIE_AFWIJKEND_PCT").alias("PREMIE_AFWIJKEND_PCT"), col("met_buitenland.PRODUCTSET_SOORT_ID").alias("PRODUCTSET_SOORT_ID"), col("met_buitenland.REFERENTIEPREMIE_BDG").alias("REFERENTIEPREMIE_BDG"), col("met_buitenland.ZORGVERZEKERING_IDC").alias("ZORGVERZEKERING_IDC"), col("met_buitenland.ZORGVERZ_OVEREENKOMST_ID").alias("ZORGVERZ_OVEREENKOMST_ID"))
