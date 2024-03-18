from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from or_hlp_peoplesoft_zorgkosten_sticky_notes.config.ConfigStore import *
from or_hlp_peoplesoft_zorgkosten_sticky_notes.udfs.UDFs import *

def Join(spark: SparkSession, STG_PST_PS_JRNL_LN_HST: DataFrame, STG_PST_PS_JRNL_LN_HEADER_HST: DataFrame) -> DataFrame:
    STG_PST_PS_JRNL_HEADER_HST.createOrReplaceTempView("STG_PST_PS_JRNL_HEADER_HST")
    STG_PST_PS_JRNL_LN_HST.createOrReplaceTempView("STG_PST_PS_JRNL_LN_HST")
    SQL_Join = spark.sql(
        f"""
WITH cal_col AS (
SELECT 
  STG_PST_PS_JRNL_LN_HST.ACCOUNT AS Kostensoort,
  STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT AS BUSINESS_UNIT,
  STG_PST_PS_JRNL_LN_HST.CHARTFIELD1 AS CHARTFIELD1,
  CONCAT(year(STG_PST_PS_JRNL_LN_HST.JOURNAL_DATE),'.',lpad(month(STG_PST_PS_JRNL_LN_HST.JOURNAL_DATE), 2, '0')) AS Jaar_Maand_Verwerking,

CASE
  WHEN SUBSTR(STG_PST_PS_JRNL_LN_HST.CHARTFIELD1, 1, 2) IN ('L0', 'V0', 'L1', 'V1', 'L2', 'V2') 
    THEN CONCAT('20', LPAD(SUBSTR(STG_PST_PS_JRNL_LN_HST.CHARTFIELD1, 2, 2), 2, '0'), '.', LPAD(SUBSTR(STG_PST_PS_JRNL_LN_HST.CHARTFIELD1, 4, 2), 2, '0'))
  WHEN SUBSTR(STG_PST_PS_JRNL_LN_HST.CHARTFIELD1, 1, 3) = 'LJR' 
    THEN CONCAT(LPAD(STG_PST_PS_JRNL_HEADER_HST.FISCAL_YEAR, 4, '0'), '.', SUBSTR(STG_PST_PS_JRNL_LN_HST.CHARTFIELD1, 4, 2))
  WHEN (coalesce(STG_PST_PS_JRNL_LN_HST.CHARTFIELD1, float('-inf')) >= 'VJR01' AND coalesce(STG_PST_PS_JRNL_LN_HST.CHARTFIELD1, float('-inf')) <= 'VJR12')
    THEN CONCAT(LPAD(STG_PST_PS_JRNL_HEADER_HST.FISCAL_YEAR - 1, 4, '0'), '.', SUBSTR(STG_PST_PS_JRNL_LN_HST.CHARTFIELD1, 4, 2))
  WHEN STG_PST_PS_JRNL_LN_HST.CHARTFIELD1 = 'VJR00'
    THEN CONCAT(LPAD(STG_PST_PS_JRNL_HEADER_HST.FISCAL_YEAR - 2, 4, '0'), '.', '00')
  ELSE CONCAT(year(STG_PST_PS_JRNL_HEADER_HST.JOURNAL_DATE),'.',lpad(month(STG_PST_PS_JRNL_HEADER_HST.JOURNAL_DATE), 2, '0') ) 
END AS Jaar_Maand_Verrichting, 

CASE
    WHEN (STG_PST_PS_JRNL_LN_HST.CHARTFIELD1 RLIKE 'T[0-9][0-9]') THEN 0
    WHEN (STG_PST_PS_JRNL_LN_HST.CHARTFIELD1 LIKE '%TAX%') THEN 0
    ELSE STG_PST_PS_JRNL_LN_HST.MONETARY_AMOUNT
  END AS Bedrag_Peoplesoft_zorgkosten,

CASE
    WHEN (STG_PST_PS_JRNL_LN_HST.CHARTFIELD1 RLIKE 'T[0-9][0-9]') 
     THEN STG_PST_PS_JRNL_LN_HST.MONETARY_AMOUNT
    WHEN (STG_PST_PS_JRNL_LN_HST.CHARTFIELD1 LIKE '%TAX%')
     THEN STG_PST_PS_JRNL_LN_HST.MONETARY_AMOUNT
    ELSE 0
END AS Bedrag_Peoplesoft_taxatie,

STG_PST_PS_JRNL_LN_HST.MONETARY_AMOUNT AS Bedrag_Peoplesoft,

CASE
    WHEN substring(upper(STG_PST_PS_JRNL_HEADER_HST.SOURCE), 1, length('M')) = 'M'
     THEN '0'
    WHEN substring(upper(STG_PST_PS_JRNL_HEADER_HST.SOURCE), 1, length('AFZ')) = 'AFZ'
     THEN '0'
ELSE '1'
END AS ind_handmatig,

CASE
    WHEN (STG_PST_PS_JRNL_LN_HST.CHARTFIELD1 RLIKE 'T[0-9][0-9]')
     THEN '1'
    WHEN STG_PST_PS_JRNL_LN_HST.CHARTFIELD1 LIKE '%TAX%'
     THEN '1'
ELSE '0'
END AS ind_taxatie,
 
CASE
    WHEN SUBSTR(STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT, 1, 3) = 'O21'
     THEN '0021'
    WHEN SUBSTR(STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT, 1, 3) = 'O20'
     THEN '0020'
    WHEN SUBSTR(STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT, 1, 3) = 'D10'
     THEN '0010'
    WHEN STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT = 'OZV01'
     THEN '0021'
    WHEN STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT = 'OZK05'
     THEN '0020'
    WHEN STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT = 'DLZ04'
     THEN '0010'
    WHEN SUBSTR(STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT, 3, 1) = '1'
     THEN '0001'
    WHEN SUBSTR(STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT, 3, 1) = '2'
     THEN '0002'
    WHEN SUBSTR(STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT, 3, 1) = '3'
     THEN '0003'
    WHEN SUBSTR(STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT, 3, 1) = '4'
     THEN '0004'
    WHEN SUBSTR(STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT, 1, 3) = 'CZZ'
     THEN '0004'
    WHEN SUBSTR(STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT, 3, 1) = '8'
     THEN '0008'
    WHEN SUBSTR(STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT, 3, 1) = '9'
     THEN '0009'
    ELSE "UNKN"
END AS KDE_JET,
CASE
    WHEN STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT = 'CC411'
     THEN '12'
    WHEN STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT = 'CM852'
     THEN '24'
    WHEN STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT = 'CR852'
     THEN '21'
    WHEN STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT = 'CR220'
     THEN '21'
    WHEN STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT = 'OZV01'
     THEN '11'
    WHEN STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT = 'OZK05'
     THEN '11'
    WHEN STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT = 'DLZ04'
     THEN '11'
    ELSE substr(STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT,4,2)
END AS VZL
FROM STG_PST_PS_JRNL_HEADER_HST INNER JOIN 
     STG_PST_PS_JRNL_LN_HST
      ON 
      (
      STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT = STG_PST_PS_JRNL_LN_HST.BUSINESS_UNIT 
      AND STG_PST_PS_JRNL_HEADER_HST.JOURNAL_ID = STG_PST_PS_JRNL_LN_HST.JOURNAL_ID 
      AND STG_PST_PS_JRNL_HEADER_HST.JRNL_HDR_STATUS IN ("P","U") 
      AND STG_PST_PS_JRNL_LN_HST.LEDGER = "GROOTB_EUR"
      )
WHERE
      (STG_PST_PS_JRNL_LN_HST.ACCOUNT BETWEEN '5000000' AND '6999999'
      or STG_PST_PS_JRNL_LN_HST.ACCOUNT IN ("116510",
      "116560", 
      "156220",
      "156230", 
      "160000" ,
      "160010" ,
      "160050" ,
      "160900" ,
      "199980" ,
      "754000" ,
      "754150" ,
      "766010" ,
      "767300" ,
      "767310" ,
      "767340" ,
      "768050" ,
      "920020" ,
      "665000",
      "691001",
      "613010"
      ))
      and STG_PST_PS_JRNL_HEADER_HST.BUSINESS_UNIT IN ("CC411","CZ411","CZ852", "CR852","CM852","D1011","D1052","O2011","O2052","O2111", "CZZ11") 
)
select * , 
IF(SUBSTR(Jaar_Maand_Verrichting, 5, 3) = '.00', '0', '1') AS ind_exact,
 CONCAT(KDE_JET, '.', VZL) AS JurEntiteit_VerzGrondsl_Code
FROM cal_col
"""
    )

    return SQL_Join
