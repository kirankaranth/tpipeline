from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Join_join(
        spark: SparkSession,
        Extract_per_blok: DataFrame,
        Join_premie_datum: DataFrame,
        SNP_LABEL: DataFrame
) -> DataFrame:
    return Extract_per_blok\
        .alias("Extract_per_blok")\
        .join(
          Join_premie_datum.alias("Join_premie_datum"),
          (
            ((col("Extract_per_blok.COLLECTIVITEIT_OVEREENKOMST_ID").eqNullSafe(col("Join_premie_datum.OVEREENKOMST_ID")) & col("Extract_per_blok.PRODUCTSET_SOORT_ID").eqNullSafe(col("Join_premie_datum.PRODUCTSET_SOORT_ID"))) & col("Extract_per_blok.BOUWSTEEN_ID").eqNullSafe(col("Join_premie_datum.BOUWSTEEN_ID")))
            & (
              ((coalesce(col("Extract_per_blok.INGANG_DTM"), lit("0001-01-01").cast(DateType())) <= coalesce(col("Join_premie_datum.EIND_DTM"), lit("0001-01-01").cast(DateType()))) & (coalesce(col("Extract_per_blok.EIND_DTM"), lit("0001-01-01").cast(DateType())) >= coalesce(col("Join_premie_datum.INGANG_DTM"), lit("0001-01-01").cast(DateType()))))
              & (
                (col("Join_premie_datum.EIGEN_RISICO_REGELING_ID").isNotNull() & col("Extract_per_blok.EIGEN_RISICO_REGELING_ID").eqNullSafe(col("Join_premie_datum.EIGEN_RISICO_REGELING_ID")))
                | col("Join_premie_datum.EIGEN_RISICO_REGELING_ID").isNull()
              )
            )
          ),
          "left_outer"
        )\
        .join(
          SNP_LABEL.alias("SNP_LABEL"),
          col("Extract_per_blok.LABEL_ID").eqNullSafe(col("SNP_LABEL.LABEL_ID")),
          "left_outer"
        )\
        .select(col("Extract_per_blok.BETAAL_TERMIJN_ID").alias("BETAAL_TERMIJN_ID"), col("Extract_per_blok.BOUWSTEEN_ID").alias("BOUWSTEEN_ID"), col("Extract_per_blok.BRN_PERSOON_ID").alias("BRN_PERSOON_ID"), col("Extract_per_blok.COLLECTIVITEIT_OVEREENKOMST_ID").alias("COLLECTIVITEIT_OVEREENKOMST_ID"), col("Join_premie_datum.COMMERCIELE_TOESLAG_BDG").alias("COMMERCIELE_TOESLAG_BDG"), col("Join_premie_datum.EDWH_RESOURCE_ID").alias("EDWH_RESOURCE_ID"), col("Extract_per_blok.EIGEN_RISICO_REGELING_ID").alias("EIGEN_RISICO_REGELING_ID"), least(col("Extract_per_blok.EIND_DTM"), col("Join_premie_datum.EIND_DTM")).alias("EIND_DTM"), col("Join_premie_datum.FABRIEKSPRIJS_BDG").alias("FABRIEKSPRIJS_BDG"), col("Extract_per_blok.GEBOORTE_DTM").alias("GEBOORTE_DTM"), col("Extract_per_blok.GESLACHT_ID").alias("GESLACHT_ID"), greatest(col("Extract_per_blok.INGANG_DTM"), col("Join_premie_datum.INGANG_DTM")).alias("INGANG_DTM"), col("Extract_per_blok.LABEL_ID").alias("LABEL_ID"), col("SNP_LABEL.LABEL_KEY").alias("LABEL_KEY"), col("Extract_per_blok.LAND_ID").alias("LAND_ID"), col("Join_premie_datum.LEEFTIJD_TOT_MET").alias("LEEFTIJD_TOT_MET"), col("Join_premie_datum.LEEFTIJD_VAN").alias("LEEFTIJD_VAN"), col("Extract_per_blok.PRODUCTSET_SOORT_ID").alias("PRODUCTSET_SOORT_ID"), col("Join_premie_datum.REFERENTIEPREMIE_BDG").alias("REFERENTIEPREMIE_BDG"), when(
          (
            expr("DAY(GREATEST(Extract_per_blok.INGANG_DTM, Join_premie_datum.INGANG_DTM))").eqNullSafe(expr("DAY(Extract_per_blok.GEBOORTE_DTM)"))
            & month(greatest(col("Extract_per_blok.INGANG_DTM"), col("Join_premie_datum.INGANG_DTM")))\
              .eqNullSafe(month(col("Extract_per_blok.GEBOORTE_DTM")))
          ), 
          (col("Extract_per_blok.GEBOORTE_DTM") + lit(1))
        )\
        .otherwise(col("Extract_per_blok.GEBOORTE_DTM"))\
        .alias("REKEN_GEBOORTE_DTM"), col("Join_premie_datum.TABELPREMIE_IDC").alias("TABELPREMIE_IDC"), col("Extract_per_blok.ZORGVERZEKERING_IDC").alias("ZORGVERZEKERING_IDC"), col("Extract_per_blok.ZORGVERZ_OVEREENKOMST_ID").alias("ZORGVERZ_OVEREENKOMST_ID"))
