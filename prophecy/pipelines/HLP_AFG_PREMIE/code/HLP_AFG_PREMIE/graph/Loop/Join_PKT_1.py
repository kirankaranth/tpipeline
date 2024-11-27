from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Join_PKT_1(spark: SparkSession, met_collectiviteit_rgl: DataFrame, laatst_vijf_jaar: DataFrame, ) -> DataFrame:
    return met_collectiviteit_rgl\
        .alias("met_collectiviteit_rgl")\
        .join(
          laatst_vijf_jaar.alias("laatst_vijf_jaar"),
          (
            ((col("met_collectiviteit_rgl.BRN_PERSOON_ID").eqNullSafe(col("laatst_vijf_jaar.BRN_PERSOON_ID")) & col("met_collectiviteit_rgl.LABEL_ID").eqNullSafe(col("laatst_vijf_jaar.LABEL_ID"))) & col("met_collectiviteit_rgl.BOUWSTEEN_ID").eqNullSafe(col("laatst_vijf_jaar.BOUWSTEEN_ID")))
            & (
              (coalesce(col("met_collectiviteit_rgl.INGANG_DTM"), lit("0001-01-01").cast(DateType())) <= coalesce(col("laatst_vijf_jaar.EIND_DTM"), lit("0001-01-01").cast(DateType())))
              & (
                coalesce(col("met_collectiviteit_rgl.EIND_DTM"), lit("0001-01-01").cast(DateType()))
                >= coalesce(col("laatst_vijf_jaar.INGANG_DTM"), lit("0001-01-01").cast(DateType()))
              )
            )
          ),
          "left_outer"
        )\
        .select(col("met_collectiviteit_rgl.BETAAL_TERMIJN_ID").alias("BETAAL_TERMIJN_ID"), col("met_collectiviteit_rgl.BOUWSTEEN_ID").alias("BOUWSTEEN_ID"), col("met_collectiviteit_rgl.BRN_PERSOON_ID").alias("BRN_PERSOON_ID"), col("met_collectiviteit_rgl.COLLECTIVITEIT_BDG").alias("COLLECTIVITEIT_BDG"), col("met_collectiviteit_rgl.COLLECTIVITEIT_OVEREENKOMST_ID").alias("COLLECTIVITEIT_OVEREENKOMST_ID"), col("met_collectiviteit_rgl.COLLECTIVITEIT_PCT").alias("COLLECTIVITEIT_PCT"), col("met_collectiviteit_rgl.COMMERCIELE_TOESLAG_BDG").alias("COMMERCIELE_TOESLAG_BDG"), col("met_collectiviteit_rgl.DETENTIE_IDC").alias("DETENTIE_IDC"), col("met_collectiviteit_rgl.EDWH_RESOURCE_ID").alias("EDWH_RESOURCE_ID"), col("met_collectiviteit_rgl.EIGEN_RISICO_REGELING_ID").alias("EIGEN_RISICO_REGELING_ID"), least(col("met_collectiviteit_rgl.EIND_DTM"), col("laatst_vijf_jaar.EIND_DTM")).alias("EIND_DTM"), col("met_collectiviteit_rgl.FABRIEKSPRIJS_BDG").alias("FABRIEKSPRIJS_BDG"), col("met_collectiviteit_rgl.GEBOORTE_DTM").alias("GEBOORTE_DTM"), col("met_collectiviteit_rgl.GESLACHT_ID").alias("GESLACHT_ID"), greatest(col("met_collectiviteit_rgl.INGANG_DTM"), col("laatst_vijf_jaar.INGANG_DTM")).alias("INGANG_DTM"), col("met_collectiviteit_rgl.LABEL_ID").alias("LABEL_ID"), col("met_collectiviteit_rgl.LAND_ID").alias("LAND_ID"), col("met_collectiviteit_rgl.LEEFTIJD_BDG").alias("LEEFTIJD_BDG"), col("met_collectiviteit_rgl.LEEFTIJD_PCT").alias("LEEFTIJD_PCT"), col("met_collectiviteit_rgl.LEEFTIJD_TOT_MET").alias("LEEFTIJD_TOT_MET"), col("met_collectiviteit_rgl.LEEFTIJD_VAN").alias("LEEFTIJD_VAN"), col("met_collectiviteit_rgl.LFT_COMM_TOESLAG_BDG").alias("LFT_COMM_TOESLAG_BDG"), col("met_collectiviteit_rgl.LFT_FABRIEKSPRIJS_BDG").alias("LFT_FABRIEKSPRIJS_BDG"), col("laatst_vijf_jaar.PAKKET_KORTING_PCT").alias("PAKKET_KORTING_PCT"), col("met_collectiviteit_rgl.PREMIE_AFWIJKEND_BDG").alias("PREMIE_AFWIJKEND_BDG"), col("met_collectiviteit_rgl.PREMIE_AFWIJKEND_PCT").alias("PREMIE_AFWIJKEND_PCT"), lit(1).alias("PRIORITEIT"), col("met_collectiviteit_rgl.PRODUCTSET_SOORT_ID").alias("PRODUCTSET_SOORT_ID"), col("met_collectiviteit_rgl.REFERENTIEPREMIE_BDG").alias("REFERENTIEPREMIE_BDG"), col("met_collectiviteit_rgl.TABELPREMIE_IDC").alias("TABELPREMIE_IDC"), col("met_collectiviteit_rgl.ZORGVERZEKERING_IDC").alias("ZORGVERZEKERING_IDC"), col("met_collectiviteit_rgl.ZORGVERZ_OVEREENKOMST_ID").alias("ZORGVERZ_OVEREENKOMST_ID"))
