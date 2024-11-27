from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from HLP_AFG_PREMIE.udfs.UDFs import *
from . import *
from .config import *


class Loop(MetaGemExec):

    def __init__(self, config):
        self.config = config
        super().__init__()

    def execute(self, spark: SparkSession, subgraph_config: SubgraphConfig) -> List[DataFrame]:
        Config.update(subgraph_config)
        df_User_Written_1 = User_Written_1(spark)
        df_source_lnk_premie = source_lnk_premie(spark)
        df_Snapshot_uit_historie = Snapshot_uit_historie(spark, df_source_lnk_premie)
        df_source_datum = source_datum(spark)
        df_Extract_filter = Extract_filter(spark, df_source_datum)
        df_Extract_aggregate = Extract_aggregate(spark, df_Extract_filter)
        df_Join = Join(spark, df_Snapshot_uit_historie, df_Extract_aggregate)
        df_source_label = source_label(spark)
        df_Snapshot_uit_historie_1 = Snapshot_uit_historie_1(spark, df_source_label)
        df_source_hlp_premie_parameters_1 = source_hlp_premie_parameters_1(spark)
        df_Extract_filter_1 = Extract_filter_1(spark, df_source_hlp_premie_parameters_1)
        df_Extract_reformat = Extract_reformat(spark, df_Extract_filter_1)
        df_Join_join = Join_join(spark, df_Extract_reformat, df_Join, df_Snapshot_uit_historie_1)
        df_Join_orderby = Join_orderby(spark, df_Join_join)
        df_Extract_filter_2 = Extract_filter_2(spark, df_Join_orderby)
        df_Extract_reformat_1 = Extract_reformat_1(spark, df_Extract_filter_2)
        df_Extract_dedup = Extract_dedup(spark, df_Extract_reformat_1)
        df_source_lnk_premie_rgl_leeftijd = source_lnk_premie_rgl_leeftijd(spark)
        df_Snapshot_uit_historie_2 = Snapshot_uit_historie_2(spark, df_source_lnk_premie_rgl_leeftijd)
        df_Join_1 = Join_1(spark, df_Extract_dedup, df_Snapshot_uit_historie_2)
        Verwijder_Work_tabellen_alleen_inputs(spark, df_Join, df_Snapshot_uit_historie_2)
        df_Extract = Extract(spark, df_Join_1)
        df_source_lnk_brn_afwijking_premie = source_lnk_brn_afwijking_premie(spark)
        df_Snapshot_uit_historie_3 = Snapshot_uit_historie_3(spark, df_source_lnk_brn_afwijking_premie)
        df_source_lnk_afwijking_premie_bouwst = source_lnk_afwijking_premie_bouwst(spark)
        df_Snapshot_uit_historie_4 = Snapshot_uit_historie_4(spark, df_source_lnk_afwijking_premie_bouwst)
        df_source_afwijking_premie_reden = source_afwijking_premie_reden(spark)
        df_Snapshot_uit_historie_5 = Snapshot_uit_historie_5(spark, df_source_afwijking_premie_reden)
        df_Join_join_1 = Join_join_1(
            spark, 
            df_Snapshot_uit_historie_3, 
            df_Snapshot_uit_historie_4, 
            df_Snapshot_uit_historie_5
        )
        df_Join_dedup = Join_dedup(spark, df_Join_join_1)
        df_Splitter_split_1 = Splitter_split_1(spark, df_Join_dedup)
        df_Splitter_split_2 = Splitter_split_2(spark, df_Join_dedup)
        df_Join_AFW_1 = Join_AFW_1(spark, df_Extract, df_Splitter_split_1, df_Splitter_split_2)
        df_Join_AFW_2 = Join_AFW_2(spark, df_Extract, df_Splitter_split_1, df_Splitter_split_2)
        df_Join_AFW_3 = Join_AFW_3(spark, df_Extract, df_Splitter_split_1, df_Splitter_split_2)
        df_Append_union = Append_union(spark, df_Join_AFW_1, df_Join_AFW_2, df_Join_AFW_3)
        df_Append_subset_cols = Append_subset_cols(spark, df_Append_union)
        df_BepaalPeriodeMetPrioriteit = BepaalPeriodeMetPrioriteit(spark, df_Append_subset_cols)
        df_source_lnk_premie_rgl_collectiviteit = source_lnk_premie_rgl_collectiviteit(spark)
        df_Snapshot_uit_historie_6 = Snapshot_uit_historie_6(spark, df_source_lnk_premie_rgl_collectiviteit)
        df_Join_2 = Join_2(spark, df_BepaalPeriodeMetPrioriteit, df_Snapshot_uit_historie_6)
        Verwijder_Work_tabellen_alleen_inputs_1(
            spark, 
            df_Join_1, 
            df_Snapshot_uit_historie_3, 
            df_Snapshot_uit_historie_4, 
            df_Snapshot_uit_historie_6
        )
        df_source_lnk_premie_rgl_pakketkorting = source_lnk_premie_rgl_pakketkorting(spark)
        df_Snapshot_uit_historie_7 = Snapshot_uit_historie_7(spark, df_source_lnk_premie_rgl_pakketkorting)
        df_Extract_filter_3 = Extract_filter_3(spark, df_Snapshot_uit_historie_7)
        df_Extract_reformat_2 = Extract_reformat_2(spark, df_Extract_filter_3)
        df_Join_PKT_1 = Join_PKT_1(spark, df_Join_2, df_Extract_reformat_2)
        df_Join_PKT_2 = Join_PKT_2(spark, df_Join_2, df_Extract_reformat_2)
        df_Join_PKT_3 = Join_PKT_3(spark, df_Join_2, df_Extract_reformat_2)
        df_Append_union_1 = Append_union_1(spark, df_Join_PKT_1, df_Join_PKT_2, df_Join_PKT_3)
        df_Append_subset_cols_1 = Append_subset_cols_1(spark, df_Append_union_1)
        df_BepaalPeriodeMetPrioriteit_1 = BepaalPeriodeMetPrioriteit_1(spark, df_Append_subset_cols_1)
        df_source_lnk_premie_rgl_eigen_risico = source_lnk_premie_rgl_eigen_risico(spark)
        df_Snapshot_uit_historie_8 = Snapshot_uit_historie_8(spark, df_source_lnk_premie_rgl_eigen_risico)
        df_Join_3 = Join_3(spark, df_BepaalPeriodeMetPrioriteit_1, df_Snapshot_uit_historie_8)
        Verwijder_Work_tabellen_alleen_inputs_2(
            spark, 
            df_Append_subset_cols, 
            df_BepaalPeriodeMetPrioriteit, 
            df_Join_2, 
            df_Snapshot_uit_historie_7, 
            df_Append_subset_cols_1, 
            df_Snapshot_uit_historie_8
        )
        df_Extract_1 = Extract_1(spark, df_Join_3)
        df_source_lnk_premie_rgl_assurantie = source_lnk_premie_rgl_assurantie(spark)
        df_Snapshot_uit_historie_9 = Snapshot_uit_historie_9(spark, df_source_lnk_premie_rgl_assurantie)
        df_Join_4 = Join_4(spark, df_Extract_1, df_Snapshot_uit_historie_9)
        df_source_lnk_premie_rgl_buitenland = source_lnk_premie_rgl_buitenland(spark)
        df_Snapshot_uit_historie_10 = Snapshot_uit_historie_10(spark, df_source_lnk_premie_rgl_buitenland)
        df_Join_5 = Join_5(spark, df_Join_4, df_Snapshot_uit_historie_10)
        Verwijder_Work_tabellen_alleen_inputs_3(
            spark, 
            df_BepaalPeriodeMetPrioriteit_1, 
            df_Join_3, 
            df_Snapshot_uit_historie_9, 
            df_Snapshot_uit_historie_10
        )
        df_Extract_2 = Extract_2(spark, df_Join_5)
        df_source_lnk_premie_rgl_eigen_product = source_lnk_premie_rgl_eigen_product(spark)
        df_Snapshot_uit_historie_11 = Snapshot_uit_historie_11(spark, df_source_lnk_premie_rgl_eigen_product)
        df_Join_6 = Join_6(spark, df_Extract_2, df_Snapshot_uit_historie_11)
        df_source_lnk_premie_rgl_betaaltermijn = source_lnk_premie_rgl_betaaltermijn(spark)
        df_Snapshot_uit_historie_12 = Snapshot_uit_historie_12(spark, df_source_lnk_premie_rgl_betaaltermijn)
        df_Join_7 = Join_7(spark, df_Join_6, df_Snapshot_uit_historie_12)
        target_tmp_hlp_afg_premie_blok__blokid(spark, df_Join_7)
        subgraph_config.update(Config)

    def apply(self, spark: SparkSession, blokgrootte: DataFrame, ) -> None:
        in0 = blokgrootte
        inDFs = []
        results = []
        conf_to_column = dict(
            [("NUMMER", "BLOKNUMMER"),  ("LAATSTE_BRN_PERSOON_ID", "EIND_BRN_PERSOON_ID"),              ("EERSTE_BRN_PERSOON_ID", "START_BRN_PERSOON_ID")]
        )

        if blokgrootte.count() > 999999999:
            raise Exception(f"Config DataFrame row count::{in0.count()} exceeds max run count")

        for row in blokgrootte.collect():
            update_config = self.config.update_from_row_map(row, conf_to_column)
            _inputs = inDFs
            results.append(self.__run__(spark, update_config, *_inputs))

        return do_union(results)
