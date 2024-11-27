from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from HLP_AFG_PREMIE.config.ConfigStore import *
from HLP_AFG_PREMIE.udfs.UDFs import *

def User_Written(spark: SparkSession, HLP_PREMIE_PARAMETERS: DataFrame) -> DataFrame:
    # Original transform id: A5H5U7VN.BX005APL
    # Original transform name: User Written
    # Original transform description: Bepalen blokgrootte
    #
    # Auto-generated input/output variables.
    # Variables for input ports
    HLP_PREMIE_PARAMETERS.createOrReplaceTempView('_INPUT')
    _input = '_INPUT'
    HLP_PREMIE_PARAMETERS.createOrReplaceTempView('_INPUT1')
    _input1 = '_INPUT1'
    # Variables for output ports
    _output = '_OUTPUT'
    # Original code:
    #  %LET BLOKGROOTTE = 2000000; /* 2 miljoen, dus ongeveer 25 loops in PRD */
    #
    # /* Tellen aantal brn_persoon_id regels */
    # proc sql;
    #   create table sort_brn_persoon as
    #   select BRN_PERSOON_ID,
    #          COUNT(*) as AANTAL_BRN_PERSOON
    #    from &_INPUT
    #   group by BRN_PERSOON_ID;
    # quit;
    #
    # /* Aanmaken blokken van aantallen bron persoon id's zoals aangegeven in de parameter */
    # /* Van belang is dat er groepen worden gemaakt met gehele bron_persoon_id's */
    # /* en niet dat dezelfde brn_persoon_id's in verschillende blokken komen */
    # data &_OUTPUT(keep=bloknummer start_brn_persoon_id eind_brn_persoon_id);
    #   retain start_brn_persoon_id eind_brn_persoon_id 0;
    #   set sort_brn_persoon end=eof;
    #   sum_aantal_brn_persoon + aantal_brn_persoon;
    #   if (sum_aantal_brn_persoon > &BLOKGROOTTE) or eof then do;
    #      start_brn_persoon_id = eind_brn_persoon_id;
    #      eind_brn_persoon_id = BRN_PERSOON_ID;
    #      bloknummer + 1;
    #      output;
    #      sum_aantal_brn_persoon = 0;
    #   end;
    # run;
    #
    # %put _user_;
    # Auto-generated variables.
    # Variables for output ports
    blokgrootte = spark.table("_OUTPUT")

    return blokgrootte
