from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from HLP_AFG_PREMIE.udfs.UDFs import *
from . import *
from .config import *


class Loop_1(MetaGemExec):

    def __init__(self, config):
        self.config = config
        super().__init__()

    def execute(self, spark: SparkSession, subgraph_config: SubgraphConfig) -> List[DataFrame]:
        Config.update(subgraph_config)
        df_User_Written_2 = User_Written_2(spark)
        df_source_tmp_hlp_afg_premie_blok__blokid = source_tmp_hlp_afg_premie_blok__blokid(spark)
        df_User_Written_3 = User_Written_3(spark, df_source_tmp_hlp_afg_premie_blok__blokid)
        target_hlp_afg_premie(spark, df_User_Written_3)
        Verwijder_tabellen_library_input(spark, df_source_tmp_hlp_afg_premie_blok__blokid)
        subgraph_config.update(Config)

    def apply(self, spark: SparkSession, blokgrootte: DataFrame, ) -> None:
        in0 = blokgrootte
        inDFs = []
        results = []
        conf_to_column = dict([("NUMMER", "BLOKNUMMER")])

        if blokgrootte.count() > 999999999:
            raise Exception(f"Config DataFrame row count::{in0.count()} exceeds max run count")

        for row in blokgrootte.collect():
            update_config = self.config.update_from_row_map(row, conf_to_column)
            _inputs = inDFs
            results.append(self.__run__(spark, update_config, *_inputs))

        return do_union(results)
