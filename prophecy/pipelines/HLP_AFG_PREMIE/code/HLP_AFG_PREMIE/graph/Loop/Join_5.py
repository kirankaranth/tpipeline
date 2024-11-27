from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Join_5(spark: SparkSession, met_ass: DataFrame, snap_bl: DataFrame, ) -> DataFrame:
    return met_ass\
        .alias("met_ass")\
        .join(
          snap_bl.alias("snap_bl"),
          (
            ((col("met_ass.COLLECTIVITEIT_OVEREENKOMST_ID").eqNullSafe(col("snap_bl.COLLECTIVITEIT_OVEREENKOMST_ID")) & col("met_ass.PRODUCTSET_SOORT_ID").eqNullSafe(col("snap_bl.PRODUCTSET_SOORT_ID"))) & col("met_ass.BOUWSTEEN_ID").eqNullSafe(col("snap_bl.BOUWSTEEN_ID")))
            & (
              (col("met_ass.LAND_ID").eqNullSafe(col("snap_bl.LAND_ID")) & (coalesce(col("met_ass.INGANG_DTM"), lit("0001-01-01").cast(DateType())) <= coalesce(col("snap_bl.EIND_DTM"), lit("0001-01-01").cast(DateType()))))
              & (
                coalesce(col("met_ass.EIND_DTM"), lit("0001-01-01").cast(DateType()))
                >= coalesce(col("snap_bl.INGANG_DTM"), lit("0001-01-01").cast(DateType()))
              )
            )
          ),
          "left_outer"
        )\
        .select(col("met_ass.ASSURANTIE_PCT").alias("ASSURANTIE_PCT"), col("met_ass.BETAAL_TERMIJN_ID").alias("BETAAL_TERMIJN_ID"), col("met_ass.BOUWSTEEN_ID").alias("BOUWSTEEN_ID"), col("met_ass.BRN_PERSOON_ID").alias("BRN_PERSOON_ID"), col("snap_bl.BUITENLAND_BDG").alias("BUITENLAND_BDG"), col("snap_bl.BUITENLAND_PCT").alias("BUITENLAND_PCT"), col("met_ass.COLLECTIVITEIT_BDG").alias("COLLECTIVITEIT_BDG"), col("met_ass.COLLECTIVITEIT_OVEREENKOMST_ID").alias("COLLECTIVITEIT_OVEREENKOMST_ID"), col("met_ass.COLLECTIVITEIT_PCT").alias("COLLECTIVITEIT_PCT"), col("met_ass.COMMERCIELE_TOESLAG_BDG").alias("COMMERCIELE_TOESLAG_BDG"), col("met_ass.DETENTIE_IDC").alias("DETENTIE_IDC"), col("met_ass.EDWH_RESOURCE_ID").alias("EDWH_RESOURCE_ID"), col("met_ass.EIGEN_RISICO_BDG").alias("EIGEN_RISICO_BDG"), col("met_ass.EIGEN_RISICO_PCT").alias("EIGEN_RISICO_PCT"), col("met_ass.EIGEN_RISICO_REGELING_ID").alias("EIGEN_RISICO_REGELING_ID"), least(col("met_ass.EIND_DTM"), col("snap_bl.EIND_DTM")).alias("EIND_DTM"), CZ_SAS_BR_T_NATUURLIJK_PERS_LEEFTIJD(col("met_ass.GEBOORTE_DTM"), expr("min(met_ass.EIND_DTM, snap_bl.EIND_DTM)"), expr(Config.SYSPEILDATUM))\
        .alias("EIND_DTM_LEEFTIJD"), col("met_ass.ER_COMM_TOESLAG_BDG").alias("ER_COMM_TOESLAG_BDG"), col("met_ass.ER_FABRIEKSPRIJS_BDG").alias("ER_FABRIEKSPRIJS_BDG"), col("met_ass.FABRIEKSPRIJS_BDG").alias("FABRIEKSPRIJS_BDG"), col("met_ass.GEBOORTE_DTM").alias("GEBOORTE_DTM"), col("met_ass.GESLACHT_ID").alias("GESLACHT_ID"), greatest(col("met_ass.INGANG_DTM"), col("snap_bl.INGANG_DTM")).alias("INGANG_DTM"), CZ_SAS_BR_T_NATUURLIJK_PERS_LEEFTIJD(col("met_ass.GEBOORTE_DTM"), expr("max(met_ass.INGANG_DTM, snap_bl.INGANG_DTM)"), expr(Config.SYSPEILDATUM))\
        .alias("INGANG_DTM_LEEFTIJD"), col("met_ass.LABEL_ID").alias("LABEL_ID"), col("met_ass.LAND_ID").alias("LAND_ID"), col("met_ass.LEEFTIJD_BDG").alias("LEEFTIJD_BDG"), (year(greatest(col("met_ass.INGANG_DTM"), col("snap_bl.INGANG_DTM"))) - year(col("met_ass.GEBOORTE_DTM")))\
        .alias("LEEFTIJD_JAAR_INGANGSDATUM"), col("met_ass.LEEFTIJD_PCT").alias("LEEFTIJD_PCT"), col("met_ass.LEEFTIJD_TOT_MET").alias("LEEFTIJD_TOT_MET"), col("snap_bl.LEEFTIJD_TOT_MET").alias("LEEFTIJD_TOT_MET_BL"), col("met_ass.LEEFTIJD_TOT_MET_ER").alias("LEEFTIJD_TOT_MET_ER"), col("met_ass.LEEFTIJD_VAN").alias("LEEFTIJD_VAN"), col("snap_bl.LEEFTIJD_VAN").alias("LEEFTIJD_VAN_BL"), col("met_ass.LEEFTIJD_VAN_ER").alias("LEEFTIJD_VAN_ER"), col("met_ass.LFT_COMM_TOESLAG_BDG").alias("LFT_COMM_TOESLAG_BDG"), col("met_ass.LFT_FABRIEKSPRIJS_BDG").alias("LFT_FABRIEKSPRIJS_BDG"), col("met_ass.PAKKET_KORTING_PCT").alias("PAKKET_KORTING_PCT"), col("met_ass.PREMIE_AFWIJKEND_BDG").alias("PREMIE_AFWIJKEND_BDG"), col("met_ass.PREMIE_AFWIJKEND_PCT").alias("PREMIE_AFWIJKEND_PCT"), col("met_ass.PRODUCTSET_SOORT_ID").alias("PRODUCTSET_SOORT_ID"), col("met_ass.REFERENTIEPREMIE_BDG").alias("REFERENTIEPREMIE_BDG"), col("met_ass.ZORGVERZEKERING_IDC").alias("ZORGVERZEKERING_IDC"), col("met_ass.ZORGVERZ_OVEREENKOMST_ID").alias("ZORGVERZ_OVEREENKOMST_ID"))
