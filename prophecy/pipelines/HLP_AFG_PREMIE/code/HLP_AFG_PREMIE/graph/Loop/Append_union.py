from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Append_union(
        spark: SparkSession,
        Met_afw_premie: DataFrame,
        periode_voor_ap: DataFrame,
        periode_na_ap: DataFrame
) -> DataFrame:
    nonEmptyDf = [x for x in [Met_afw_premie, periode_voor_ap, periode_na_ap] if x is not None]
    res = nonEmptyDf[0]
    rest = nonEmptyDf[1:]

    for inDF in rest:
        res = res.unionByName(inDF, allowMissingColumns = True)

    return res
