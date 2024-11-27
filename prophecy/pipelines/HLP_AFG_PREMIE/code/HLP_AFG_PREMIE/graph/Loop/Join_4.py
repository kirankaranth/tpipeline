from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Join_4(spark: SparkSession, met_ER_regel: DataFrame, snap_ass: DataFrame, ) -> DataFrame:
    return met_ER_regel\
        .alias("met_ER_regel")\
        .join(
          snap_ass.alias("snap_ass"),
          (
            ((col("met_ER_regel.LABEL_ID").eqNullSafe(col("snap_ass.LABEL_ID")) & col("met_ER_regel.BOUWSTEEN_ID").eqNullSafe(col("snap_ass.BOUWSTEEN_ID"))) & col("met_ER_regel.LAND_ID").eqNullSafe(col("snap_ass.LAND_ID")))
            & (
              (coalesce(col("met_ER_regel.ER_INGANG_DTM"), lit("0001-01-01").cast(DateType())) <= coalesce(col("snap_ass.EIND_DTM"), lit("0001-01-01").cast(DateType())))
              & (
                coalesce(col("met_ER_regel.ER_EIND_DTM"), lit("0001-01-01").cast(DateType()))
                >= coalesce(col("snap_ass.INGANG_DTM"), lit("0001-01-01").cast(DateType()))
              )
            )
          ),
          "left_outer"
        )\
        .select(col("snap_ass.ASSURANTIE_PCT").alias("ASSURANTIE_PCT"), col("met_ER_regel.BETAAL_TERMIJN_ID").alias("BETAAL_TERMIJN_ID"), col("met_ER_regel.BOUWSTEEN_ID").alias("BOUWSTEEN_ID"), col("met_ER_regel.BRN_PERSOON_ID").alias("BRN_PERSOON_ID"), col("met_ER_regel.COLLECTIVITEIT_BDG").alias("COLLECTIVITEIT_BDG"), col("met_ER_regel.COLLECTIVITEIT_OVEREENKOMST_ID").alias("COLLECTIVITEIT_OVEREENKOMST_ID"), col("met_ER_regel.COLLECTIVITEIT_PCT").alias("COLLECTIVITEIT_PCT"), col("met_ER_regel.COMMERCIELE_TOESLAG_BDG").alias("COMMERCIELE_TOESLAG_BDG"), col("met_ER_regel.DETENTIE_IDC").alias("DETENTIE_IDC"), col("met_ER_regel.EDWH_RESOURCE_ID").alias("EDWH_RESOURCE_ID"), col("met_ER_regel.EIGEN_RISICO_BDG").alias("EIGEN_RISICO_BDG"), col("met_ER_regel.EIGEN_RISICO_PCT").alias("EIGEN_RISICO_PCT"), col("met_ER_regel.EIGEN_RISICO_REGELING_ID").alias("EIGEN_RISICO_REGELING_ID"), least(col("met_ER_regel.ER_EIND_DTM"), col("snap_ass.EIND_DTM")).alias("EIND_DTM"), col("met_ER_regel.ER_COMM_TOESLAG_BDG").alias("ER_COMM_TOESLAG_BDG"), col("met_ER_regel.ER_FABRIEKSPRIJS_BDG").alias("ER_FABRIEKSPRIJS_BDG"), col("met_ER_regel.FABRIEKSPRIJS_BDG").alias("FABRIEKSPRIJS_BDG"), col("met_ER_regel.GEBOORTE_DTM").alias("GEBOORTE_DTM"), col("met_ER_regel.GESLACHT_ID").alias("GESLACHT_ID"), greatest(col("met_ER_regel.ER_INGANG_DTM"), col("snap_ass.INGANG_DTM")).alias("INGANG_DTM"), col("met_ER_regel.LABEL_ID").alias("LABEL_ID"), col("met_ER_regel.LAND_ID").alias("LAND_ID"), col("met_ER_regel.LEEFTIJD_BDG").alias("LEEFTIJD_BDG"), col("met_ER_regel.LEEFTIJD_PCT").alias("LEEFTIJD_PCT"), col("met_ER_regel.LEEFTIJD_TOT_MET").alias("LEEFTIJD_TOT_MET"), col("met_ER_regel.LEEFTIJD_TOT_MET_ER").alias("LEEFTIJD_TOT_MET_ER"), col("met_ER_regel.LEEFTIJD_VAN").alias("LEEFTIJD_VAN"), col("met_ER_regel.LEEFTIJD_VAN_ER").alias("LEEFTIJD_VAN_ER"), col("met_ER_regel.LFT_COMM_TOESLAG_BDG").alias("LFT_COMM_TOESLAG_BDG"), col("met_ER_regel.LFT_FABRIEKSPRIJS_BDG").alias("LFT_FABRIEKSPRIJS_BDG"), col("met_ER_regel.PAKKET_KORTING_PCT").alias("PAKKET_KORTING_PCT"), col("met_ER_regel.PREMIE_AFWIJKEND_BDG").alias("PREMIE_AFWIJKEND_BDG"), col("met_ER_regel.PREMIE_AFWIJKEND_PCT").alias("PREMIE_AFWIJKEND_PCT"), col("met_ER_regel.PRODUCTSET_SOORT_ID").alias("PRODUCTSET_SOORT_ID"), col("met_ER_regel.REFERENTIEPREMIE_BDG").alias("REFERENTIEPREMIE_BDG"), col("met_ER_regel.ZORGVERZEKERING_IDC").alias("ZORGVERZEKERING_IDC"), col("met_ER_regel.ZORGVERZ_OVEREENKOMST_ID").alias("ZORGVERZ_OVEREENKOMST_ID"))
