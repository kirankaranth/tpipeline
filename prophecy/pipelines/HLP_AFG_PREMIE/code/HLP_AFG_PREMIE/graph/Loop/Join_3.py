from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Join_3(spark: SparkSession, BepaalPeriodeMetPrioriteit: DataFrame, snap_rgl_eigenrisico: DataFrame, ) -> DataFrame:
    return BepaalPeriodeMetPrioriteit\
        .alias("BepaalPeriodeMetPrioriteit")\
        .join(
          snap_rgl_eigenrisico.alias("snap_rgl_eigenrisico"),
          (
            ((col("BepaalPeriodeMetPrioriteit.COLLECTIVITEIT_OVEREENKOMST_ID").eqNullSafe(col("snap_rgl_eigenrisico.COLLECTIVITEIT_OVEREENKOMST_ID")) & col("BepaalPeriodeMetPrioriteit.PRODUCTSET_SOORT_ID").eqNullSafe(col("snap_rgl_eigenrisico.PRODUCTSET_SOORT_ID"))) & (col("BepaalPeriodeMetPrioriteit.BOUWSTEEN_ID").eqNullSafe(col("snap_rgl_eigenrisico.BOUWSTEEN_ID")) & col("BepaalPeriodeMetPrioriteit.EIGEN_RISICO_REGELING_ID").eqNullSafe(col("snap_rgl_eigenrisico.EIGEN_RISICO_REGELING_ID"))))
            & (
              ((coalesce(col("BepaalPeriodeMetPrioriteit.INGANG_RELEVANTE_DTM"), lit("0001-01-01").cast(DateType())) <= coalesce(col("snap_rgl_eigenrisico.EIND_DTM"), lit("0001-01-01").cast(DateType()))) & (coalesce(col("BepaalPeriodeMetPrioriteit.EIND_RELEVANTE_DTM"), lit("0001-01-01").cast(DateType())) >= coalesce(col("snap_rgl_eigenrisico.INGANG_DTM"), lit("0001-01-01").cast(DateType()))))
              & ~ col("BepaalPeriodeMetPrioriteit.TABELPREMIE_IDC").eqNullSafe(lit(1))
            )
          ),
          "left_outer"
        )\
        .select(col("BepaalPeriodeMetPrioriteit.BETAAL_TERMIJN_ID").alias("BETAAL_TERMIJN_ID"), col("BepaalPeriodeMetPrioriteit.BOUWSTEEN_ID").alias("BOUWSTEEN_ID"), col("BepaalPeriodeMetPrioriteit.BRN_PERSOON_ID").alias("BRN_PERSOON_ID"), col("BepaalPeriodeMetPrioriteit.COLLECTIVITEIT_BDG").alias("COLLECTIVITEIT_BDG"), col("BepaalPeriodeMetPrioriteit.COLLECTIVITEIT_OVEREENKOMST_ID").alias("COLLECTIVITEIT_OVEREENKOMST_ID"), col("BepaalPeriodeMetPrioriteit.COLLECTIVITEIT_PCT").alias("COLLECTIVITEIT_PCT"), col("BepaalPeriodeMetPrioriteit.COMMERCIELE_TOESLAG_BDG").alias("COMMERCIELE_TOESLAG_BDG"), col("BepaalPeriodeMetPrioriteit.DETENTIE_IDC").alias("DETENTIE_IDC"), col("BepaalPeriodeMetPrioriteit.EDWH_RESOURCE_ID").alias("EDWH_RESOURCE_ID"), col("snap_rgl_eigenrisico.EIGEN_RISICO_BDG").alias("EIGEN_RISICO_BDG"), col("snap_rgl_eigenrisico.EIGEN_RISICO_PCT").alias("EIGEN_RISICO_PCT"), col("BepaalPeriodeMetPrioriteit.EIGEN_RISICO_REGELING_ID").alias("EIGEN_RISICO_REGELING_ID"), least(col("BepaalPeriodeMetPrioriteit.EIND_RELEVANTE_DTM"), col("snap_rgl_eigenrisico.EIND_DTM"))\
        .alias("EIND_DTM"), CZ_SAS_BR_T_NATUURLIJK_PERS_LEEFTIJD(
          col("BepaalPeriodeMetPrioriteit.GEBOORTE_DTM"), 
          expr("min(W973D84.eind_relevante_dtm, snap_rgl_eigenrisico.EIND_DTM)"), 
          expr(Config.SYSPEILDATUM)
        )\
        .alias("EIND_DTM_LEEFTIJD"), col("snap_rgl_eigenrisico.COMM_TOESLAG_BDG").alias("ER_COMM_TOESLAG_BDG"), col("snap_rgl_eigenrisico.FABRIEKSPRIJS_BDG").alias("ER_FABRIEKSPRIJS_BDG"), col("BepaalPeriodeMetPrioriteit.FABRIEKSPRIJS_BDG").alias("FABRIEKSPRIJS_BDG"), col("BepaalPeriodeMetPrioriteit.GEBOORTE_DTM").alias("GEBOORTE_DTM"), col("BepaalPeriodeMetPrioriteit.GESLACHT_ID").alias("GESLACHT_ID"), greatest(col("BepaalPeriodeMetPrioriteit.INGANG_RELEVANTE_DTM"), col("snap_rgl_eigenrisico.INGANG_DTM"))\
        .alias("INGANG_DTM"), CZ_SAS_BR_T_NATUURLIJK_PERS_LEEFTIJD(
          col("BepaalPeriodeMetPrioriteit.GEBOORTE_DTM"), 
          expr("max(W973D84.ingang_relevante_dtm, snap_rgl_eigenrisico.INGANG_DTM)"), 
          expr(Config.SYSPEILDATUM)
        )\
        .alias("INGANG_DTM_LEEFTIJD"), col("BepaalPeriodeMetPrioriteit.LABEL_ID").alias("LABEL_ID"), col("BepaalPeriodeMetPrioriteit.LAND_ID").alias("LAND_ID"), col("BepaalPeriodeMetPrioriteit.LEEFTIJD_BDG").alias("LEEFTIJD_BDG"), (
        year(greatest(col("BepaalPeriodeMetPrioriteit.INGANG_RELEVANTE_DTM"), col("snap_rgl_eigenrisico.INGANG_DTM")))
        - year(col("BepaalPeriodeMetPrioriteit.GEBOORTE_DTM"))
    )\
        .alias(
        "LEEFTIJD_JAAR_INGANGSDATUM"
    ), col("BepaalPeriodeMetPrioriteit.LEEFTIJD_PCT").alias("LEEFTIJD_PCT"), col("BepaalPeriodeMetPrioriteit.LEEFTIJD_TOT_MET").alias("LEEFTIJD_TOT_MET"), col("snap_rgl_eigenrisico.LEEFTIJD_TOT_MET").alias("LEEFTIJD_TOT_MET_ER"), col("BepaalPeriodeMetPrioriteit.LEEFTIJD_VAN").alias("LEEFTIJD_VAN"), col("snap_rgl_eigenrisico.LEEFTIJD_VAN").alias("LEEFTIJD_VAN_ER"), col("BepaalPeriodeMetPrioriteit.LFT_COMM_TOESLAG_BDG").alias("LFT_COMM_TOESLAG_BDG"), col("BepaalPeriodeMetPrioriteit.LFT_FABRIEKSPRIJS_BDG").alias("LFT_FABRIEKSPRIJS_BDG"), col("BepaalPeriodeMetPrioriteit.PAKKET_KORTING_PCT").alias("PAKKET_KORTING_PCT"), col("BepaalPeriodeMetPrioriteit.PREMIE_AFWIJKEND_BDG").alias("PREMIE_AFWIJKEND_BDG"), col("BepaalPeriodeMetPrioriteit.PREMIE_AFWIJKEND_PCT").alias("PREMIE_AFWIJKEND_PCT"), col("BepaalPeriodeMetPrioriteit.PRODUCTSET_SOORT_ID").alias("PRODUCTSET_SOORT_ID"), col("BepaalPeriodeMetPrioriteit.REFERENTIEPREMIE_BDG").alias("REFERENTIEPREMIE_BDG"), col("BepaalPeriodeMetPrioriteit.ZORGVERZEKERING_IDC").alias("ZORGVERZEKERING_IDC"), col("BepaalPeriodeMetPrioriteit.ZORGVERZ_OVEREENKOMST_ID").alias("ZORGVERZ_OVEREENKOMST_ID"))
