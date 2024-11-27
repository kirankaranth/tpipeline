from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Join_7(spark: SparkSession, met_eigen_prod: DataFrame, snap_bet_term: DataFrame, ) -> DataFrame:
    return met_eigen_prod\
        .alias("met_eigen_prod")\
        .join(
          snap_bet_term.alias("snap_bet_term"),
          (
            ((col("met_eigen_prod.COLLECTIVITEIT_OVEREENKOMST_ID").eqNullSafe(col("snap_bet_term.COLLECTIVITEIT_OVEREENKOMST_ID")) & col("met_eigen_prod.PRODUCTSET_SOORT_ID").eqNullSafe(col("snap_bet_term.PRODUCTSET_SOORT_ID"))) & col("met_eigen_prod.BOUWSTEEN_ID").eqNullSafe(col("snap_bet_term.BOUWSTEEN_ID")))
            & (
              (col("met_eigen_prod.BETAAL_TERMIJN_ID").eqNullSafe(col("snap_bet_term.BETAAL_TERMIJN_ID")) & (coalesce(col("met_eigen_prod.INGANG_DTM"), lit("0001-01-01").cast(DateType())) <= coalesce(col("snap_bet_term.EIND_DTM"), lit("0001-01-01").cast(DateType()))))
              & (
                coalesce(col("met_eigen_prod.EIND_DTM"), lit("0001-01-01").cast(DateType()))
                >= coalesce(col("snap_bet_term.INGANG_DTM"), lit("0001-01-01").cast(DateType()))
              )
            )
          ),
          "left_outer"
        )\
        .select(col("met_eigen_prod.ASSURANTIE_PCT").alias("ASSURANTIE_PCT"), col("snap_bet_term.BETAALTERMIJN_PCT").alias("BETAALTERMIJN_PCT"), col("met_eigen_prod.BETAAL_TERMIJN_ID").alias("BETAAL_TERMIJN_ID"), col("met_eigen_prod.BOUWSTEEN_ID").alias("BOUWSTEEN_ID"), col("met_eigen_prod.BRN_PERSOON_ID").alias("BRN_PERSOON_ID"), col("met_eigen_prod.BUITENLAND_BDG").alias("BUITENLAND_BDG"), col("met_eigen_prod.BUITENLAND_PCT").alias("BUITENLAND_PCT"), col("met_eigen_prod.COLLECTIVITEIT_BDG").alias("COLLECTIVITEIT_BDG"), col("met_eigen_prod.COLLECTIVITEIT_OVEREENKOMST_ID").alias("COLLECTIVITEIT_OVEREENKOMST_ID"), col("met_eigen_prod.COLLECTIVITEIT_PCT").alias("COLLECTIVITEIT_PCT"), col("met_eigen_prod.COMMERCIELE_TOESLAG_BDG").alias("COMMERCIELE_TOESLAG_BDG"), col("met_eigen_prod.DETENTIE_IDC").alias("DETENTIE_IDC"), col("met_eigen_prod.EDWH_RESOURCE_ID").alias("EDWH_RESOURCE_ID"), col("met_eigen_prod.EIGEN_PRODUCT_BDG").alias("EIGEN_PRODUCT_BDG"), col("met_eigen_prod.EIGEN_PRODUCT_PCT").alias("EIGEN_PRODUCT_PCT"), col("met_eigen_prod.EIGEN_RISICO_BDG").alias("EIGEN_RISICO_BDG"), col("met_eigen_prod.EIGEN_RISICO_PCT").alias("EIGEN_RISICO_PCT"), col("met_eigen_prod.EIGEN_RISICO_REGELING_ID").alias("EIGEN_RISICO_REGELING_ID"), least(col("met_eigen_prod.EIND_DTM"), col("snap_bet_term.EIND_DTM")).alias("EIND_DTM"), col("met_eigen_prod.ER_COMM_TOESLAG_BDG").alias("ER_COMM_TOESLAG_BDG"), col("met_eigen_prod.ER_FABRIEKSPRIJS_BDG").alias("ER_FABRIEKSPRIJS_BDG"), col("met_eigen_prod.FABRIEKSPRIJS_BDG").alias("FABRIEKSPRIJS_BDG"), col("met_eigen_prod.GEBOORTE_DTM").alias("GEBOORTE_DTM"), col("met_eigen_prod.GESLACHT_ID").alias("GESLACHT_ID"), greatest(col("met_eigen_prod.INGANG_DTM"), col("snap_bet_term.INGANG_DTM")).alias("INGANG_DTM"), col("met_eigen_prod.LABEL_ID").alias("LABEL_ID"), col("met_eigen_prod.LAND_ID").alias("LAND_ID"), col("met_eigen_prod.LEEFTIJD_BDG").alias("LEEFTIJD_BDG"), col("met_eigen_prod.LEEFTIJD_PCT").alias("LEEFTIJD_PCT"), col("met_eigen_prod.LEEFTIJD_TOT_MET").alias("LEEFTIJD_TOT_MET"), col("met_eigen_prod.LEEFTIJD_TOT_MET_BL").alias("LEEFTIJD_TOT_MET_BL"), col("met_eigen_prod.LEEFTIJD_TOT_MET_ER").alias("LEEFTIJD_TOT_MET_ER"), col("met_eigen_prod.LEEFTIJD_VAN").alias("LEEFTIJD_VAN"), col("met_eigen_prod.LEEFTIJD_VAN_BL").alias("LEEFTIJD_VAN_BL"), col("met_eigen_prod.LEEFTIJD_VAN_ER").alias("LEEFTIJD_VAN_ER"), col("met_eigen_prod.LFT_COMM_TOESLAG_BDG").alias("LFT_COMM_TOESLAG_BDG"), col("met_eigen_prod.LFT_FABRIEKSPRIJS_BDG").alias("LFT_FABRIEKSPRIJS_BDG"), col("met_eigen_prod.PAKKET_KORTING_PCT").alias("PAKKET_KORTING_PCT"), col("met_eigen_prod.PREMIE_AFWIJKEND_BDG").alias("PREMIE_AFWIJKEND_BDG"), col("met_eigen_prod.PREMIE_AFWIJKEND_PCT").alias("PREMIE_AFWIJKEND_PCT"), col("met_eigen_prod.PRODUCTSET_SOORT_ID").alias("PRODUCTSET_SOORT_ID"), col("met_eigen_prod.REFERENTIEPREMIE_BDG").alias("REFERENTIEPREMIE_BDG"), col("met_eigen_prod.ZORGVERZEKERING_IDC").alias("ZORGVERZEKERING_IDC"), col("met_eigen_prod.ZORGVERZ_OVEREENKOMST_ID").alias("ZORGVERZ_OVEREENKOMST_ID"))
