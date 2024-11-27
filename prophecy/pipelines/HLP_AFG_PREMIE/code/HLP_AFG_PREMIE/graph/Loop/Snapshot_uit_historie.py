from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Snapshot_uit_historie(spark: SparkSession, LNK_PREMIE: DataFrame) -> DataFrame:
    import pyspark.sql.functions as pf
    import json

    def snapshot_from_history(spark: SparkSession, params: dict) -> dict:
        result = {}

        if "source" in params:
            source_df: DataFrame = params["source"][0]
        else:
            raise Exception("no source")

        if "history_key" in params:
            history_key = params.get("history_key")
        else:
            raise Exception("no history_key")

        max_value_field = ""

        if "max_value_field" in params:
            max_value_field = params.get("max_value_field")

        keep_deleted = False

        if "keep_deleted" in params:
            keep_deleted = params.get("keep_deleted")

        filter_string = ""

        if "filter_string" in params:
            filter_string = params.get("filter_string")

        snapshot_date = ""

        if "snapshot_date" in params:
            snapshot_date = params["snapshot_date"]
        else:
            raise Exception("no snapshot_date")

        # except:
        #     raise Exception("Invalid snapshot_date")
        # extract max value, if required
        if max_value_field:
            result = {
                "max_value": source_df\
                  .filter(coalesce(col("EDWH_PEIL_DTM"), lit("0001-01-01").cast("date")) <= snapshot_date)\
                  .agg(max(max_value_field).alias(max_value_field))\
                  .first()[max_value_field]
            }

            # print(max_value)
            # setattr(Config, "SNAPSHOT_FROM_HISTORY_MAX_VALUE", max_value)
            if (hasattr(Config, "SNAPSHOT_FROM_HISTORY_MAX_VALUE") and Config.SNAPSHOT_FROM_HISTORY_MAX_VALUE):
                snapshot_from_history_max_value = {}

                if isinstance(Config.SNAPSHOT_FROM_HISTORY_MAX_VALUE, ConfigTree):
                    snapshot_from_history_max_value = dict(Config.SNAPSHOT_FROM_HISTORY_MAX_VALUE)
                elif isinstance(Config.SNAPSHOT_FROM_HISTORY_MAX_VALUE, str):
                    snapshot_from_history_max_value = json.loads(Config.SNAPSHOT_FROM_HISTORY_MAX_VALUE)
            else:
                snapshot_from_history_max_value = {}

            # print(snapshot_from_history_max_value)
            snapshot_from_history_max_value[f"MAX_{max_value_field.upper()}"] = source_df\
                                                                                    .filter(
                                                                                      (
                                                                                        coalesce(col("EDWH_PEIL_DTM"), lit("0001-01-01").cast("date"))
                                                                                        <= snapshot_date
                                                                                      )
                                                                                    )\
                                                                                    .agg(
                                                                                      max(max_value_field)\
                                                                                        .alias(max_value_field)
                                                                                    )\
                                                                                    .first()[max_value_field] if source_df\
                                                                                    .filter(
                                                                                      (
                                                                                        coalesce(col("EDWH_PEIL_DTM"), lit("0001-01-01").cast("date"))
                                                                                        <= snapshot_date
                                                                                      )
                                                                                    )\
                                                                                    .agg(
                                                                                      max(max_value_field)\
                                                                                        .alias(max_value_field)
                                                                                    )\
                                                                                    .first()[max_value_field] else 0
            setattr(Config, "SNAPSHOT_FROM_HISTORY_MAX_VALUE", json.dumps(snapshot_from_history_max_value), )

        # keep only first row in the assorted key
        target_df = (source_df\
                        .filter(coalesce(col("EDWH_PEIL_DTM"), lit("0001-01-01").cast("date")) <= snapshot_date)\
                        .withColumn(
                          "row_number",
                          row_number()\
                            .over(Window\
                            .partitionBy([c.strip() for c in history_key.split(",")])\
                            .orderBy([
                            pf.col(c).desc()
                            for c in (
                              [c.strip() for c in "EDWH_PEIL_DTM,EDWH_PROCES_DTD,EDWH_VERWIJDER_DTD".split(",")]
                              + sorted(
                                [
                                  x
                                  for x in sorted(
                                    [
                                      c.upper()
                                      for c in source_df.filter(
                                        coalesce(col("EDWH_PEIL_DTM"), lit("0001-01-01").cast("date")) <= snapshot_date
                                      )\
                                        .columns
                                    ]
                                  )
                                  if (
                                  x
                                  not in [c.strip() for c in "EDWH_PEIL_DTM,EDWH_PROCES_DTD,EDWH_VERWIJDER_DTD".split(",")]
                                )
                                ]
                              )
                            )
                          ]))
                        )\
                        .where(
                          (
                            "row_number == 1 and (EDWH_VERWIJDER_DTD is null or "
                            + ("1=1" if keep_deleted else "1=2")
                            + ")"
                          )
                        )\
                        .drop("row_number"))

        # apply additional filter if required
        if filter_string:
            target_df = (source_df\
                            .filter(coalesce(col("EDWH_PEIL_DTM"), lit("0001-01-01").cast("date")) <= snapshot_date)\
                            .withColumn(
                              "row_number",
                              row_number()\
                                .over(Window\
                                .partitionBy([c.strip() for c in history_key.split(",")])\
                                .orderBy([
                                pf.col(c).desc()
                                for c in (
                                  [c.strip() for c in "EDWH_PEIL_DTM,EDWH_PROCES_DTD,EDWH_VERWIJDER_DTD".split(",")]
                                  + sorted(
                                    [
                                      x
                                      for x in sorted(
                                        [
                                          c.upper()
                                          for c in source_df.filter(
                                            (
                                              coalesce(col("EDWH_PEIL_DTM"), lit("0001-01-01").cast("date"))
                                              <= snapshot_date
                                            )
                                          )\
                                            .columns
                                        ]
                                      )
                                      if (
                                      x
                                      not in [
                                        c.strip()
                                        for c in "EDWH_PEIL_DTM,EDWH_PROCES_DTD,EDWH_VERWIJDER_DTD".split(",")
                                      ]
                                    )
                                    ]
                                  )
                                )
                              ]))
                            )\
                            .where(
                              (
                                "row_number == 1 and (EDWH_VERWIJDER_DTD is null or "
                                + ("1=1" if keep_deleted else "1=2")
                                + ")"
                              )
                            )\
                            .drop("row_number"))\
                            .filter(filter_string)

        # disabled as it was in SAS
        # result["target"] = [target_df.select(columns_to_keep.split(","))]
        result["target"] = [target_df]
        result["is_error"] = False

        return result

    return snapshot_from_history(
        spark,
        {
          "source": [LNK_PREMIE],
          "history_key": "LNK_PREMIE_ID",
          "columns_to_keep": "LNK_PREMIE_ID,OVEREENKOMST_ID,PRODUCTSET_SOORT_ID,BOUWSTEEN_ID,INGANG_DTM,EIND_DTM,REFERENTIEPREMIE_BDG,FABRIEKSPRIJS_BDG,COMMERCIELE_TOESLAG_BDG,EDWH_RESOURCE_ID,EDWH_PEIL_DTM,EDWH_VERWIJDER_DTD,EDWH_PROCES_DTD,EIGEN_RISICO_REGELING_ID,TABELPREMIE_IDC,LEEFTIJD_VAN,LEEFTIJD_TOT_MET",
          "keep_deleted": False,
          "snapshot_date": CZ_SAS_HUIDIGE_SITUATIE(expr(Config.SYSPEILDATUM)),
          "snapshot_date_format": "yyyy-MM-dd",
          "max_value_field": "",
        }
    )["target"][0]
