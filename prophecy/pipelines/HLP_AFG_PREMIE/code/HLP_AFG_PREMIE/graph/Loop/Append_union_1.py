from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def Append_union_1(
        spark: SparkSession,
        met_pakketkorting: DataFrame,
        periode_voor_pk: DataFrame,
        periode_na_pk: DataFrame
) -> DataFrame:
    nonEmptyDf = [x for x in [met_pakketkorting, periode_voor_pk, periode_na_pk] if x is not None]
    res = nonEmptyDf[0]
    rest = nonEmptyDf[1:]

    for inDF in rest:
        res = res.unionByName(inDF, allowMissingColumns = True)

    return res
