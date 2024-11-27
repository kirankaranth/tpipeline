from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Join_AFW_1(
        spark: SparkSession,
        met_leeftijdregel: DataFrame,
        afw_premie_zonder: DataFrame,
        afw_premie_bouwsteen: DataFrame
) -> DataFrame:
    return met_leeftijdregel\
        .alias("met_leeftijdregel")\
        .join(
          afw_premie_zonder.alias("afw_premie_zonder"),
          (
            ((col("met_leeftijdregel.BRN_PERSOON_ID").eqNullSafe(col("afw_premie_zonder.BRN_PERSOON_ID")) & col("met_leeftijdregel.ZORGVERZ_OVEREENKOMST_ID").eqNullSafe(col("afw_premie_zonder.OVEREENKOMST_ID"))) & (coalesce(col("met_leeftijdregel.LFT_INGANG_DTM"), lit("0001-01-01").cast(DateType())) <= coalesce(col("afw_premie_zonder.EIND_DTM"), lit("0001-01-01").cast(DateType()))))
            & (
              (coalesce(col("met_leeftijdregel.LFT_EIND_DTM"), lit("0001-01-01").cast(DateType())) >= coalesce(col("afw_premie_zonder.INGANG_DTM"), lit("0001-01-01").cast(DateType())))
              & col("afw_premie_zonder.BOUWSTEEN_ID").isNull()
            )
          ),
          "left_outer"
        )\
        .join(
          afw_premie_bouwsteen.alias("afw_premie_bouwsteen"),
          (
            ((col("met_leeftijdregel.BRN_PERSOON_ID").eqNullSafe(col("afw_premie_bouwsteen.BRN_PERSOON_ID")) & col("met_leeftijdregel.ZORGVERZ_OVEREENKOMST_ID").eqNullSafe(col("afw_premie_bouwsteen.OVEREENKOMST_ID"))) & col("met_leeftijdregel.BOUWSTEEN_ID").eqNullSafe(col("afw_premie_bouwsteen.BOUWSTEEN_ID")))
            & (
              ((coalesce(col("met_leeftijdregel.LFT_INGANG_DTM"), lit("0001-01-01").cast(DateType())) <= coalesce(col("afw_premie_bouwsteen.EIND_DTM"), lit("0001-01-01").cast(DateType()))) & (coalesce(col("met_leeftijdregel.LFT_EIND_DTM"), lit("0001-01-01").cast(DateType())) >= coalesce(col("afw_premie_bouwsteen.INGANG_DTM"), lit("0001-01-01").cast(DateType()))))
              & col("afw_premie_bouwsteen.BOUWSTEEN_ID").isNotNull()
            )
          ),
          "left_outer"
        )\
        .select(col("met_leeftijdregel.BETAAL_TERMIJN_ID").alias("BETAAL_TERMIJN_ID"), col("met_leeftijdregel.BOUWSTEEN_ID").alias("BOUWSTEEN_ID"), col("met_leeftijdregel.BRN_PERSOON_ID").alias("BRN_PERSOON_ID"), col("met_leeftijdregel.COLLECTIVITEIT_OVEREENKOMST_ID").alias("COLLECTIVITEIT_OVEREENKOMST_ID"), when(
          rtrim(
              coalesce(
                col("afw_premie_zonder.AFWIJKING_PREMIE_REDEN_CODE"), 
                col("afw_premie_bouwsteen.AFWIJKING_PREMIE_REDEN_CODE")
              )
            )\
            .isin(lit("001"), lit("011")), 
          lit(0)
        )\
        .otherwise(col("met_leeftijdregel.COMMERCIELE_TOESLAG_BDG"))\
        .alias("COMMERCIELE_TOESLAG_BDG"), coalesce(col("afw_premie_zonder.DETENTIE_IDC"), col("afw_premie_bouwsteen.DETENTIE_IDC"), lit(0))\
        .alias("DETENTIE_IDC"), col("met_leeftijdregel.EDWH_RESOURCE_ID").alias("EDWH_RESOURCE_ID"), col("met_leeftijdregel.EIGEN_RISICO_REGELING_ID").alias("EIGEN_RISICO_REGELING_ID"), least(col("met_leeftijdregel.LFT_EIND_DTM"), col("afw_premie_zonder.EIND_DTM"), col("afw_premie_bouwsteen.EIND_DTM"))\
        .alias("EIND_DTM"), when(
          rtrim(
              coalesce(
                col("afw_premie_zonder.AFWIJKING_PREMIE_REDEN_CODE"), 
                col("afw_premie_bouwsteen.AFWIJKING_PREMIE_REDEN_CODE")
              )
            )\
            .isin(lit("001"), lit("011")), 
          lit(0)
        )\
        .otherwise(col("met_leeftijdregel.FABRIEKSPRIJS_BDG"))\
        .alias("FABRIEKSPRIJS_BDG"), col("met_leeftijdregel.GEBOORTE_DTM").alias("GEBOORTE_DTM"), col("met_leeftijdregel.GESLACHT_ID").alias("GESLACHT_ID"), greatest(
          col("met_leeftijdregel.LFT_INGANG_DTM"), 
          col("afw_premie_zonder.INGANG_DTM"), 
          col("afw_premie_bouwsteen.INGANG_DTM")
        )\
        .alias("INGANG_DTM"), col("met_leeftijdregel.LABEL_ID").alias("LABEL_ID"), col("met_leeftijdregel.LAND_ID").alias("LAND_ID"), col("met_leeftijdregel.LEEFTIJD_BDG").alias("LEEFTIJD_BDG"), col("met_leeftijdregel.LEEFTIJD_PCT").alias("LEEFTIJD_PCT"), col("met_leeftijdregel.LEEFTIJD_TOT_MET").alias("LEEFTIJD_TOT_MET"), col("met_leeftijdregel.LEEFTIJD_VAN").alias("LEEFTIJD_VAN"), col("met_leeftijdregel.LFT_COMM_TOESLAG_BDG").alias("LFT_COMM_TOESLAG_BDG"), col("met_leeftijdregel.LFT_FABRIEKSPRIJS_BDG").alias("LFT_FABRIEKSPRIJS_BDG"), coalesce(col("afw_premie_zonder.PREMIE_AFWIJKEND_BDG"), col("afw_premie_bouwsteen.PREMIE_AFWIJKEND_BDG"))\
        .alias("PREMIE_AFWIJKEND_BDG"), coalesce(col("afw_premie_zonder.PREMIE_AFWIJKEND_PCT"), col("afw_premie_bouwsteen.PREMIE_AFWIJKEND_PCT"))\
        .alias("PREMIE_AFWIJKEND_PCT"), lit(1).alias("PRIORITEIT"), col("met_leeftijdregel.PRODUCTSET_SOORT_ID").alias("PRODUCTSET_SOORT_ID"), when(
          rtrim(
              coalesce(
                col("afw_premie_zonder.AFWIJKING_PREMIE_REDEN_CODE"), 
                col("afw_premie_bouwsteen.AFWIJKING_PREMIE_REDEN_CODE")
              )
            )\
            .isin(lit("001"), lit("011")), 
          lit(0)
        )\
        .otherwise(col("met_leeftijdregel.REFERENTIEPREMIE_BDG"))\
        .alias("REFERENTIEPREMIE_BDG"), col("met_leeftijdregel.TABELPREMIE_IDC").alias("TABELPREMIE_IDC"), col("met_leeftijdregel.ZORGVERZEKERING_IDC").alias("ZORGVERZEKERING_IDC"), col("met_leeftijdregel.ZORGVERZ_OVEREENKOMST_ID").alias("ZORGVERZ_OVEREENKOMST_ID"))
