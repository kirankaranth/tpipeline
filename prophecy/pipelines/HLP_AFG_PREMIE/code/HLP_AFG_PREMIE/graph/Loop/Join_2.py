from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Join_2(
        spark: SparkSession,
        BepaalPeriodeMetPrioriteit_OUTPUT0: DataFrame,
        snap_rgl_collectiviteit: DataFrame,
) -> DataFrame:
    return BepaalPeriodeMetPrioriteit_OUTPUT0\
        .alias("BepaalPeriodeMetPrioriteit_OUTPUT0")\
        .join(
          snap_rgl_collectiviteit.alias("snap_rgl_collectiviteit"),
          (
            ((col("BepaalPeriodeMetPrioriteit_OUTPUT0.COLLECTIVITEIT_OVEREENKOMST_ID").eqNullSafe(col("snap_rgl_collectiviteit.COLLECTIVITEIT_OVEREENKOMST_ID")) & col("BepaalPeriodeMetPrioriteit_OUTPUT0.PRODUCTSET_SOORT_ID").eqNullSafe(col("snap_rgl_collectiviteit.PRODUCTSET_SOORT_ID"))) & col("BepaalPeriodeMetPrioriteit_OUTPUT0.BOUWSTEEN_ID").eqNullSafe(col("snap_rgl_collectiviteit.BOUWSTEEN_ID")))
            & (
              (coalesce(col("BepaalPeriodeMetPrioriteit_OUTPUT0.INGANG_RELEVANTE_DTM"), lit("0001-01-01").cast(DateType())) <= coalesce(col("snap_rgl_collectiviteit.EIND_DTM"), lit("0001-01-01").cast(DateType())))
              & (
                coalesce(col("BepaalPeriodeMetPrioriteit_OUTPUT0.EIND_RELEVANTE_DTM"), lit("0001-01-01").cast(DateType()))
                >= coalesce(col("snap_rgl_collectiviteit.INGANG_DTM"), lit("0001-01-01").cast(DateType()))
              )
            )
          ),
          "left_outer"
        )\
        .select(col("BepaalPeriodeMetPrioriteit_OUTPUT0.BETAAL_TERMIJN_ID").alias("BETAAL_TERMIJN_ID"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.BOUWSTEEN_ID").alias("BOUWSTEEN_ID"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.BRN_PERSOON_ID").alias("BRN_PERSOON_ID"), col("snap_rgl_collectiviteit.COLLECTIVITEIT_BDG").alias("COLLECTIVITEIT_BDG"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.COLLECTIVITEIT_OVEREENKOMST_ID")\
        .alias("COLLECTIVITEIT_OVEREENKOMST_ID"), col("snap_rgl_collectiviteit.COLLECTIVITEIT_PCT").alias("COLLECTIVITEIT_PCT"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.COMMERCIELE_TOESLAG_BDG").alias("COMMERCIELE_TOESLAG_BDG"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.DETENTIE_IDC").alias("DETENTIE_IDC"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.EDWH_RESOURCE_ID").alias("EDWH_RESOURCE_ID"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.EIGEN_RISICO_REGELING_ID").alias("EIGEN_RISICO_REGELING_ID"), least(col("BepaalPeriodeMetPrioriteit_OUTPUT0.EIND_RELEVANTE_DTM"), col("snap_rgl_collectiviteit.EIND_DTM"))\
        .alias("EIND_DTM"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.FABRIEKSPRIJS_BDG").alias("FABRIEKSPRIJS_BDG"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.GEBOORTE_DTM").alias("GEBOORTE_DTM"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.GESLACHT_ID").alias("GESLACHT_ID"), greatest(col("BepaalPeriodeMetPrioriteit_OUTPUT0.INGANG_RELEVANTE_DTM"), col("snap_rgl_collectiviteit.INGANG_DTM"))\
        .alias("INGANG_DTM"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.LABEL_ID").alias("LABEL_ID"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.LAND_ID").alias("LAND_ID"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.LEEFTIJD_BDG").alias("LEEFTIJD_BDG"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.LEEFTIJD_PCT").alias("LEEFTIJD_PCT"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.LEEFTIJD_TOT_MET").alias("LEEFTIJD_TOT_MET"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.LEEFTIJD_VAN").alias("LEEFTIJD_VAN"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.LFT_COMM_TOESLAG_BDG").alias("LFT_COMM_TOESLAG_BDG"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.LFT_FABRIEKSPRIJS_BDG").alias("LFT_FABRIEKSPRIJS_BDG"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.PREMIE_AFWIJKEND_BDG").alias("PREMIE_AFWIJKEND_BDG"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.PREMIE_AFWIJKEND_PCT").alias("PREMIE_AFWIJKEND_PCT"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.PRODUCTSET_SOORT_ID").alias("PRODUCTSET_SOORT_ID"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.REFERENTIEPREMIE_BDG").alias("REFERENTIEPREMIE_BDG"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.TABELPREMIE_IDC").alias("TABELPREMIE_IDC"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.ZORGVERZEKERING_IDC").alias("ZORGVERZEKERING_IDC"), col("BepaalPeriodeMetPrioriteit_OUTPUT0.ZORGVERZ_OVEREENKOMST_ID").alias("ZORGVERZ_OVEREENKOMST_ID"))
