from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.lookups import (
    createLookup,
    createRangeLookup,
    lookup,
    lookup_last,
    lookup_match,
    lookup_count,
    lookup_row,
    lookup_row_reverse,
    lookup_nth
)

def registerUDFs(spark: SparkSession):
    spark.udf.register("CZ_SAS_ARIS2SASDATE", CZ_SAS_ARIS2SASDATE)
    spark.udf.register("CZ_SAS_BEREKEN_PREMIE", CZ_SAS_BEREKEN_PREMIE)
    spark.udf.register("CZ_SAS_BEREKEN_REGELING", CZ_SAS_BEREKEN_REGELING)
    spark.udf.register("CZ_SAS_BR_BEPAAL_INCPROCESSTAP_CREATIEN", CZ_SAS_BR_BEPAAL_INCPROCESSTAP_CREATIEN)
    spark.udf.register("CZ_SAS_BR_BEPAAL_INCPROCESSTAP_DUMMY", CZ_SAS_BR_BEPAAL_INCPROCESSTAP_DUMMY)
    spark.udf.register("CZ_SAS_BR_BEPAAL_INCPROCESSTAP_FINTRANS", CZ_SAS_BR_BEPAAL_INCPROCESSTAP_FINTRANS)
    spark.udf.register("CZ_SAS_BR_BEPAAL_INCPROCESSTAP_ZWAARTE", CZ_SAS_BR_BEPAAL_INCPROCESSTAP_ZWAARTE)
    spark.udf.register("CZ_SAS_BR_BEREKEN_OPENSTAAND_BEDRAG", CZ_SAS_BR_BEREKEN_OPENSTAAND_BEDRAG)
    spark.udf.register("CZ_SAS_BR_BUS_UNIT_2_JE_VZL", CZ_SAS_BR_BUS_UNIT_2_JE_VZL)
    spark.udf.register("CZ_SAS_BR_BUS_UNIT_2_JUR_ENT", CZ_SAS_BR_BUS_UNIT_2_JUR_ENT)
    spark.udf.register("CZ_SAS_BR_B_OPENST_EXCL_ONINB_BDG", CZ_SAS_BR_B_OPENST_EXCL_ONINB_BDG)
    spark.udf.register("CZ_SAS_BR_CLASSIFY_CHARACTERS", CZ_SAS_BR_CLASSIFY_CHARACTERS)
    spark.udf.register("CZ_SAS_BR_COULANCE_SOORT", CZ_SAS_BR_COULANCE_SOORT)
    spark.udf.register("CZ_SAS_BR_CREEER_INCASSO_FASE_CODE", CZ_SAS_BR_CREEER_INCASSO_FASE_CODE)
    spark.udf.register("CZ_SAS_BR_CREEER_OUDERDOM_CAT_CODE", CZ_SAS_BR_CREEER_OUDERDOM_CAT_CODE)
    spark.udf.register("CZ_SAS_BR_CVZ_REGELING_IDC", CZ_SAS_BR_CVZ_REGELING_IDC)
    spark.udf.register("CZ_SAS_BR_EXCL_GESPREID_BETALEN_ER", CZ_SAS_BR_EXCL_GESPREID_BETALEN_ER)
    spark.udf.register("CZ_SAS_BR_EXCL_KRUISDOSSIER", CZ_SAS_BR_EXCL_KRUISDOSSIER)
    spark.udf.register("CZ_SAS_BR_EXCL_SOMMATIEDOSSIER", CZ_SAS_BR_EXCL_SOMMATIEDOSSIER)
    spark.udf.register("CZ_SAS_BR_FIN_DEB_STARTDTM_DM", CZ_SAS_BR_FIN_DEB_STARTDTM_DM)
    spark.udf.register("CZ_SAS_BR_F_AFDELING_MACHTIGING", CZ_SAS_BR_F_AFDELING_MACHTIGING)
    spark.udf.register("CZ_SAS_BR_F_ATL_BEHANDELINGEN_BOEKING", CZ_SAS_BR_F_ATL_BEHANDELINGEN_BOEKING)
    spark.udf.register("CZ_SAS_BR_F_CRMZ_LEEFTIJD_TM", CZ_SAS_BR_F_CRMZ_LEEFTIJD_TM)
    spark.udf.register("CZ_SAS_BR_F_DBC_AANTAL", CZ_SAS_BR_F_DBC_AANTAL)
    spark.udf.register("CZ_SAS_BR_F_EMAILEXTBESTAND", CZ_SAS_BR_F_EMAILEXTBESTAND)
    spark.udf.register("CZ_SAS_BR_F_GDS_PRCODELIJST_GGZ", CZ_SAS_BR_F_GDS_PRCODELIJST_GGZ)
    spark.udf.register("CZ_SAS_BR_F_GDS_PRCODELIJST_PARAMEDISCH", CZ_SAS_BR_F_GDS_PRCODELIJST_PARAMEDISCH)
    spark.udf.register("CZ_SAS_BR_F_GELDIGHEID_STATUS", CZ_SAS_BR_F_GELDIGHEID_STATUS)
    spark.udf.register("CZ_SAS_BR_F_LABEL_VERSTREKKING", CZ_SAS_BR_F_LABEL_VERSTREKKING)
    spark.udf.register("CZ_SAS_BR_F_LEIDENDE_ZORGVERLENER", CZ_SAS_BR_F_LEIDENDE_ZORGVERLENER)
    spark.udf.register("CZ_SAS_BR_F_PENSIOENLEEFTIJD_MENV", CZ_SAS_BR_F_PENSIOENLEEFTIJD_MENV)
    spark.udf.register("CZ_SAS_BR_F_PERSONEEL_COLLECTIVITEIT_ID", CZ_SAS_BR_F_PERSONEEL_COLLECTIVITEIT_ID)
    spark.udf.register("CZ_SAS_BR_F_PRESTATIE_ATL_BOEKING", CZ_SAS_BR_F_PRESTATIE_ATL_BOEKING)
    spark.udf.register("CZ_SAS_BR_F_SRT_VERZEKERING_OMS", CZ_SAS_BR_F_SRT_VERZEKERING_OMS)
    spark.udf.register("CZ_SAS_BR_F_UKGRGL_PARTITIONS", CZ_SAS_BR_F_UKGRGL_PARTITIONS)
    spark.udf.register("CZ_SAS_BR_F_ZKA_DIVISIE_FILTER", CZ_SAS_BR_F_ZKA_DIVISIE_FILTER)
    spark.udf.register("CZ_SAS_BR_F_ZKA_SECTOR", CZ_SAS_BR_F_ZKA_SECTOR)
    spark.udf.register("CZ_SAS_BR_IND_ALLEEN_AV", CZ_SAS_BR_IND_ALLEEN_AV)
    spark.udf.register("CZ_SAS_BR_IND_AV", CZ_SAS_BR_IND_AV)
    spark.udf.register("CZ_SAS_BR_IND_AV_COLLECTIEF", CZ_SAS_BR_IND_AV_COLLECTIEF)
    spark.udf.register("CZ_SAS_BR_IND_AV_COLLECTIEF_RECHTSTREEKS", CZ_SAS_BR_IND_AV_COLLECTIEF_RECHTSTREEKS)
    spark.udf.register("CZ_SAS_BR_IND_AV_COLLECTIEF_TUSSENPERSOON", CZ_SAS_BR_IND_AV_COLLECTIEF_TUSSENPERSOON)
    spark.udf.register("CZ_SAS_BR_IND_AV_INDIVIDUEEL", CZ_SAS_BR_IND_AV_INDIVIDUEEL)
    spark.udf.register("CZ_SAS_BR_IND_AV_INDIVIDUEEL_RECHTSTREEKS", CZ_SAS_BR_IND_AV_INDIVIDUEEL_RECHTSTREEKS)
    spark.udf.register("CZ_SAS_BR_IND_AV_INDIVIDUEEL_TUSSENPERSOON", CZ_SAS_BR_IND_AV_INDIVIDUEEL_TUSSENPERSOON)
    spark.udf.register("CZ_SAS_BR_IND_AV_PARTICULIER", CZ_SAS_BR_IND_AV_PARTICULIER)
    spark.udf.register("CZ_SAS_BR_IND_AV_VERDRAG", CZ_SAS_BR_IND_AV_VERDRAG)
    spark.udf.register("CZ_SAS_BR_IND_BASIS", CZ_SAS_BR_IND_BASIS)
    spark.udf.register("CZ_SAS_BR_IND_BASIS_PARTICULIER", CZ_SAS_BR_IND_BASIS_PARTICULIER)
    spark.udf.register("CZ_SAS_BR_IND_BASIS_VERDRAG", CZ_SAS_BR_IND_BASIS_VERDRAG)
    spark.udf.register("CZ_SAS_BR_IND_COLLECTIEF", CZ_SAS_BR_IND_COLLECTIEF)
    spark.udf.register("CZ_SAS_BR_IND_COLLECTIEF_RECHTSTREEKS", CZ_SAS_BR_IND_COLLECTIEF_RECHTSTREEKS)
    spark.udf.register("CZ_SAS_BR_IND_COLLECTIEF_TUSSENPERSOON", CZ_SAS_BR_IND_COLLECTIEF_TUSSENPERSOON)
    spark.udf.register("CZ_SAS_BR_IND_INDIVIDUEEL", CZ_SAS_BR_IND_INDIVIDUEEL)
    spark.udf.register("CZ_SAS_BR_IND_INDIVIDUEEL_RECHTSTREEKS", CZ_SAS_BR_IND_INDIVIDUEEL_RECHTSTREEKS)
    spark.udf.register("CZ_SAS_BR_IND_INDIVIDUEEL_TUSSENPERSOON", CZ_SAS_BR_IND_INDIVIDUEEL_TUSSENPERSOON)
    spark.udf.register("CZ_SAS_BR_IND_JONGER_DAN_18", CZ_SAS_BR_IND_JONGER_DAN_18)
    spark.udf.register("CZ_SAS_BR_IND_VERDRAG_GEEN_RECHT", CZ_SAS_BR_IND_VERDRAG_GEEN_RECHT)
    spark.udf.register("CZ_SAS_BR_REMOVE_CONTROL_CHARACTERS", CZ_SAS_BR_REMOVE_CONTROL_CHARACTERS)
    spark.udf.register("CZ_SAS_BR_REMOVE_DOUBLE_OTHER_CHARACTERS", CZ_SAS_BR_REMOVE_DOUBLE_OTHER_CHARACTERS)
    spark.udf.register("CZ_SAS_BR_SELECT_BET_REG_EINDE", CZ_SAS_BR_SELECT_BET_REG_EINDE)
    spark.udf.register("CZ_SAS_BR_SELECT_BET_REG_START", CZ_SAS_BR_SELECT_BET_REG_START)
    spark.udf.register("CZ_SAS_BR_SEPA", CZ_SAS_BR_SEPA)
    spark.udf.register("CZ_SAS_BR_SUBSTITUTE_BETWEEN_DOUBLE_N", CZ_SAS_BR_SUBSTITUTE_BETWEEN_DOUBLE_N)
    spark.udf.register("CZ_SAS_BR_SUBSTITUTE_DOUBLE_A", CZ_SAS_BR_SUBSTITUTE_DOUBLE_A)
    spark.udf.register("CZ_SAS_BR_S_DEBITEURENBEHEER", CZ_SAS_BR_S_DEBITEURENBEHEER)
    spark.udf.register("CZ_SAS_BR_S_DEB_START_FIN_MUT", CZ_SAS_BR_S_DEB_START_FIN_MUT)
    spark.udf.register("CZ_SAS_BR_S_EXCLHUIDIGEMAAND", CZ_SAS_BR_S_EXCLHUIDIGEMAAND)
    spark.udf.register("CZ_SAS_BR_S_FINTRANSSOORT_OPBOEKING", CZ_SAS_BR_S_FINTRANSSOORT_OPBOEKING)
    spark.udf.register("CZ_SAS_BR_S_FIN_TRANS_OPBOEKING", CZ_SAS_BR_S_FIN_TRANS_OPBOEKING)
    spark.udf.register("CZ_SAS_BR_S_FIN_TRANS_STORNERING", CZ_SAS_BR_S_FIN_TRANS_STORNERING)
    spark.udf.register("CZ_SAS_BR_S_GENORM_WERKAANBOD", CZ_SAS_BR_S_GENORM_WERKAANBOD)
    spark.udf.register("CZ_SAS_BR_S_GENORM_WERKAANB_EXCLMATCH", CZ_SAS_BR_S_GENORM_WERKAANB_EXCLMATCH)
    spark.udf.register("CZ_SAS_BR_S_HERHAALVERKEER", CZ_SAS_BR_S_HERHAALVERKEER)
    spark.udf.register("CZ_SAS_BR_S_NOTAS_HERBINCASSOKST", CZ_SAS_BR_S_NOTAS_HERBINCASSOKST)
    spark.udf.register("CZ_SAS_BR_S_RUBRIEKENBESTAND", CZ_SAS_BR_S_RUBRIEKENBESTAND)
    spark.udf.register("CZ_SAS_BR_S_TELEFONIE_CENTRAALNR", CZ_SAS_BR_S_TELEFONIE_CENTRAALNR)
    spark.udf.register("CZ_SAS_BR_S_TM_VORIGJAAR", CZ_SAS_BR_S_TM_VORIGJAAR)
    spark.udf.register("CZ_SAS_BR_TV_SUBLABEL_NIET_AANWEZIG", CZ_SAS_BR_TV_SUBLABEL_NIET_AANWEZIG)
    spark.udf.register("CZ_SAS_BR_T_AANTAL_DBC", CZ_SAS_BR_T_AANTAL_DBC)
    spark.udf.register("CZ_SAS_BR_T_AANTAL_DDD", CZ_SAS_BR_T_AANTAL_DDD)
    spark.udf.register("CZ_SAS_BR_T_AANTAL_UREN", CZ_SAS_BR_T_AANTAL_UREN)
    spark.udf.register("CZ_SAS_BR_T_AANTAL_UREN_GEBOORTEZORG", CZ_SAS_BR_T_AANTAL_UREN_GEBOORTEZORG)
    spark.udf.register("CZ_SAS_BR_T_AANTAL_UREN_WIJKVERPLEGING", CZ_SAS_BR_T_AANTAL_UREN_WIJKVERPLEGING)
    spark.udf.register("CZ_SAS_BR_T_ACTUALITEIT_VERZEKERING", CZ_SAS_BR_T_ACTUALITEIT_VERZEKERING)
    spark.udf.register("CZ_SAS_BR_T_ADRES_STATUS", CZ_SAS_BR_T_ADRES_STATUS)
    spark.udf.register("CZ_SAS_BR_T_ADRES_TYPE", CZ_SAS_BR_T_ADRES_TYPE)
    spark.udf.register("CZ_SAS_BR_T_AFSLUITPROVISIE", CZ_SAS_BR_T_AFSLUITPROVISIE)
    spark.udf.register("CZ_SAS_BR_T_ANVA_POLISKEY", CZ_SAS_BR_T_ANVA_POLISKEY)
    spark.udf.register("CZ_SAS_BR_T_ANVA_SCHADEKEY", CZ_SAS_BR_T_ANVA_SCHADEKEY)
    spark.udf.register("CZ_SAS_BR_T_ANYDT", CZ_SAS_BR_T_ANYDT)
    spark.udf.register("CZ_SAS_BR_T_ARTNR_VERGOEDING_04", CZ_SAS_BR_T_ARTNR_VERGOEDING_04)
    spark.udf.register("CZ_SAS_BR_T_ARTNR_VERGOEDING_07", CZ_SAS_BR_T_ARTNR_VERGOEDING_07)
    spark.udf.register("CZ_SAS_BR_T_BDG_BTW_TRF", CZ_SAS_BR_T_BDG_BTW_TRF)
    spark.udf.register("CZ_SAS_BR_T_BDG_EB0_NTR", CZ_SAS_BR_T_BDG_EB0_NTR)
    spark.udf.register("CZ_SAS_BR_T_BDG_RST_TEE", CZ_SAS_BR_T_BDG_RST_TEE)
    spark.udf.register("CZ_SAS_BR_T_BEPAAL_KARAKTER_TYPE", CZ_SAS_BR_T_BEPAAL_KARAKTER_TYPE)
    spark.udf.register("CZ_SAS_BR_T_BEPAAL_VERZEKERAAR", CZ_SAS_BR_T_BEPAAL_VERZEKERAAR)
    spark.udf.register("CZ_SAS_BR_T_BEPAAL_ZORGVERLENER_ID", CZ_SAS_BR_T_BEPAAL_ZORGVERLENER_ID)
    spark.udf.register("CZ_SAS_BR_T_BETAALTERMIJN", CZ_SAS_BR_T_BETAALTERMIJN)
    spark.udf.register("CZ_SAS_BR_T_BGK_ACTIVITEIT", CZ_SAS_BR_T_BGK_ACTIVITEIT)
    spark.udf.register("CZ_SAS_BR_T_BGK_TYPE", CZ_SAS_BR_T_BGK_TYPE)
    spark.udf.register("CZ_SAS_BR_T_BSN", CZ_SAS_BR_T_BSN)
    spark.udf.register("CZ_SAS_BR_T_CATX", CZ_SAS_BR_T_CATX)
    spark.udf.register("CZ_SAS_BR_T_CAT_INSTELLING", CZ_SAS_BR_T_CAT_INSTELLING)
    spark.udf.register("CZ_SAS_BR_T_CONTINUITEITSPROVISIE", CZ_SAS_BR_T_CONTINUITEITSPROVISIE)
    spark.udf.register("CZ_SAS_BR_T_CORRECTIE_NTA", CZ_SAS_BR_T_CORRECTIE_NTA)
    spark.udf.register("CZ_SAS_BR_T_CORRECTIE_NTR", CZ_SAS_BR_T_CORRECTIE_NTR)
    spark.udf.register("CZ_SAS_BR_T_CRMZ_PEILDATUM_VNESTAND", CZ_SAS_BR_T_CRMZ_PEILDATUM_VNESTAND)
    spark.udf.register("CZ_SAS_BR_T_CRMZ_PEILDATUM_VRZSTAND", CZ_SAS_BR_T_CRMZ_PEILDATUM_VRZSTAND)
    spark.udf.register("CZ_SAS_BR_T_CZA_PERSONEELSNMR", CZ_SAS_BR_T_CZA_PERSONEELSNMR)
    spark.udf.register("CZ_SAS_BR_T_DATUM", CZ_SAS_BR_T_DATUM)
    spark.udf.register("CZ_SAS_BR_T_DATUMTIJD_DATEPART", CZ_SAS_BR_T_DATUMTIJD_DATEPART)
    spark.udf.register("CZ_SAS_BR_T_DATUM_TIJD", CZ_SAS_BR_T_DATUM_TIJD)
    spark.udf.register("CZ_SAS_BR_T_DBC_PERIODE", CZ_SAS_BR_T_DBC_PERIODE)
    spark.udf.register("CZ_SAS_BR_T_DDD", CZ_SAS_BR_T_DDD)
    spark.udf.register("CZ_SAS_BR_T_DD_HEROPEND_IDC", CZ_SAS_BR_T_DD_HEROPEND_IDC)
    spark.udf.register("CZ_SAS_BR_T_DD_TOEKOMST_IDC", CZ_SAS_BR_T_DD_TOEKOMST_IDC)
    spark.udf.register("CZ_SAS_BR_T_DD_XTRA_INFO_IDC", CZ_SAS_BR_T_DD_XTRA_INFO_IDC)
    spark.udf.register("CZ_SAS_BR_T_DECLARATIECODE_OVP", CZ_SAS_BR_T_DECLARATIECODE_OVP)
    spark.udf.register("CZ_SAS_BR_T_DECLARATIESOORT", CZ_SAS_BR_T_DECLARATIESOORT)
    spark.udf.register("CZ_SAS_BR_T_DV_PERSONEELSNUMMER", CZ_SAS_BR_T_DV_PERSONEELSNUMMER)
    spark.udf.register("CZ_SAS_BR_T_EIGEN_RISICO_ZVW", CZ_SAS_BR_T_EIGEN_RISICO_ZVW)
    spark.udf.register("CZ_SAS_BR_T_ELFPROEF", CZ_SAS_BR_T_ELFPROEF)
    spark.udf.register("CZ_SAS_BR_T_EMAIL_AKTIVITEIT", CZ_SAS_BR_T_EMAIL_AKTIVITEIT)
    spark.udf.register("CZ_SAS_BR_T_EMAIL_STATUS", CZ_SAS_BR_T_EMAIL_STATUS)
    spark.udf.register("CZ_SAS_BR_T_FICTIEF_LEEGMAKEN", CZ_SAS_BR_T_FICTIEF_LEEGMAKEN)
    spark.udf.register("CZ_SAS_BR_T_FOUT_ONZEKERHEID_STATUS", CZ_SAS_BR_T_FOUT_ONZEKERHEID_STATUS)
    spark.udf.register("CZ_SAS_BR_T_GECONTRACTEERD", CZ_SAS_BR_T_GECONTRACTEERD)
    spark.udf.register("CZ_SAS_BR_T_GESLACHT", CZ_SAS_BR_T_GESLACHT)
    spark.udf.register("CZ_SAS_BR_T_GPH_CODE", CZ_SAS_BR_T_GPH_CODE)
    spark.udf.register("CZ_SAS_BR_T_HOOFDLETTERS", CZ_SAS_BR_T_HOOFDLETTERS)
    spark.udf.register("CZ_SAS_BR_T_HUISNUMMER", CZ_SAS_BR_T_HUISNUMMER)
    spark.udf.register("CZ_SAS_BR_T_HUISNUMMER_TOEVOEGING", CZ_SAS_BR_T_HUISNUMMER_TOEVOEGING)
    spark.udf.register("CZ_SAS_BR_T_INDICATIE_SOORT", CZ_SAS_BR_T_INDICATIE_SOORT)
    spark.udf.register("CZ_SAS_BR_T_INDICATOR", CZ_SAS_BR_T_INDICATOR)
    spark.udf.register("CZ_SAS_BR_T_INGEDIEND_DOOR_VERZEKERDE_I", CZ_SAS_BR_T_INGEDIEND_DOOR_VERZEKERDE_I)
    spark.udf.register("CZ_SAS_BR_T_INKOOPKANAAL_TYPE", CZ_SAS_BR_T_INKOOPKANAAL_TYPE)
    spark.udf.register("CZ_SAS_BR_T_INKOOPKANAAL_TYPE_NUM", CZ_SAS_BR_T_INKOOPKANAAL_TYPE_NUM)
    spark.udf.register("CZ_SAS_BR_T_INSTITUTIONELE_ZVLSOORT", CZ_SAS_BR_T_INSTITUTIONELE_ZVLSOORT)
    spark.udf.register("CZ_SAS_BR_T_KETENZORG_SOORT", CZ_SAS_BR_T_KETENZORG_SOORT)
    spark.udf.register("CZ_SAS_BR_T_KEY_FOUT_DBC", CZ_SAS_BR_T_KEY_FOUT_DBC)
    spark.udf.register("CZ_SAS_BR_T_KEY_OVERFINANCIERING", CZ_SAS_BR_T_KEY_OVERFINANCIERING)
    spark.udf.register("CZ_SAS_BR_T_KM", CZ_SAS_BR_T_KM)
    spark.udf.register("CZ_SAS_BR_T_KOSTENSOORT_OMS", CZ_SAS_BR_T_KOSTENSOORT_OMS)
    spark.udf.register("CZ_SAS_BR_T_KREDIETTERMIJNCATEGORIE", CZ_SAS_BR_T_KREDIETTERMIJNCATEGORIE)
    spark.udf.register("CZ_SAS_BR_T_LABEL", CZ_SAS_BR_T_LABEL)
    spark.udf.register("CZ_SAS_BR_T_MASKEREN_TIJD_INTERVAL", CZ_SAS_BR_T_MASKEREN_TIJD_INTERVAL)
    spark.udf.register("CZ_SAS_BR_T_MA_AFGEHANDELD_IDC", CZ_SAS_BR_T_MA_AFGEHANDELD_IDC)
    spark.udf.register("CZ_SAS_BR_T_MA_BEPAAL_AFDELING", CZ_SAS_BR_T_MA_BEPAAL_AFDELING)
    spark.udf.register("CZ_SAS_BR_T_MA_BINNEN_DE_NORM", CZ_SAS_BR_T_MA_BINNEN_DE_NORM)
    spark.udf.register("CZ_SAS_BR_T_MA_DOORLOOPTIJD_DAGEN", CZ_SAS_BR_T_MA_DOORLOOPTIJD_DAGEN)
    spark.udf.register("CZ_SAS_BR_T_MA_DOORLOOPTIJD_WERKDAGEN", CZ_SAS_BR_T_MA_DOORLOOPTIJD_WERKDAGEN)
    spark.udf.register("CZ_SAS_BR_T_MA_EXTRA_INFO_IDC", CZ_SAS_BR_T_MA_EXTRA_INFO_IDC)
    spark.udf.register("CZ_SAS_BR_T_MA_GROEPEER_STATUS", CZ_SAS_BR_T_MA_GROEPEER_STATUS)
    spark.udf.register("CZ_SAS_BR_T_MA_HEROPEND_IDC", CZ_SAS_BR_T_MA_HEROPEND_IDC)
    spark.udf.register("CZ_SAS_BR_T_MA_INTOEKOMST_IDC", CZ_SAS_BR_T_MA_INTOEKOMST_IDC)
    spark.udf.register("CZ_SAS_BR_T_MA_KANAAL", CZ_SAS_BR_T_MA_KANAAL)
    spark.udf.register("CZ_SAS_BR_T_MA_LAND", CZ_SAS_BR_T_MA_LAND)
    spark.udf.register("CZ_SAS_BR_T_MA_PZP_IDC", CZ_SAS_BR_T_MA_PZP_IDC)
    spark.udf.register("CZ_SAS_BR_T_MA_TOEGEWEZEN_IDC", CZ_SAS_BR_T_MA_TOEGEWEZEN_IDC)
    spark.udf.register("CZ_SAS_BR_T_MA_VERWIJDEREN_IDC", CZ_SAS_BR_T_MA_VERWIJDEREN_IDC)
    spark.udf.register("CZ_SAS_BR_T_MA_VOORGELEGD_IDC", CZ_SAS_BR_T_MA_VOORGELEGD_IDC)
    spark.udf.register("CZ_SAS_BR_T_MA_ZVL_CONCAT", CZ_SAS_BR_T_MA_ZVL_CONCAT)
    spark.udf.register("CZ_SAS_BR_T_MIN_VERWIJDER_DTD", CZ_SAS_BR_T_MIN_VERWIJDER_DTD)
    spark.udf.register("CZ_SAS_BR_T_NAAM", CZ_SAS_BR_T_NAAM)
    spark.udf.register("CZ_SAS_BR_T_NATUURLIJK_PERS_LEEFTIJD", CZ_SAS_BR_T_NATUURLIJK_PERS_LEEFTIJD)
    spark.udf.register("CZ_SAS_BR_T_NORM_NAAR_UREN", CZ_SAS_BR_T_NORM_NAAR_UREN)
    spark.udf.register("CZ_SAS_BR_T_NUMERICFIELD_TOCHAR9", CZ_SAS_BR_T_NUMERICFIELD_TOCHAR9)
    spark.udf.register("CZ_SAS_BR_T_NUMERIEK", CZ_SAS_BR_T_NUMERIEK)
    spark.udf.register("CZ_SAS_BR_T_NVT_LEEGMAKEN", CZ_SAS_BR_T_NVT_LEEGMAKEN)
    spark.udf.register("CZ_SAS_BR_T_ONTSLEUTELEN", CZ_SAS_BR_T_ONTSLEUTELEN)
    spark.udf.register("CZ_SAS_BR_T_POSTCODE", CZ_SAS_BR_T_POSTCODE)
    spark.udf.register("CZ_SAS_BR_T_PREMIEGRONDSLAG_BDG", CZ_SAS_BR_T_PREMIEGRONDSLAG_BDG)
    spark.udf.register("CZ_SAS_BR_T_PREMIEPLICHTIG", CZ_SAS_BR_T_PREMIEPLICHTIG)
    spark.udf.register("CZ_SAS_BR_T_PREMIEVRIJE_VERZ_IDC", CZ_SAS_BR_T_PREMIEVRIJE_VERZ_IDC)
    spark.udf.register("CZ_SAS_BR_T_PROVISIE_REGELING_BDG", CZ_SAS_BR_T_PROVISIE_REGELING_BDG)
    spark.udf.register("CZ_SAS_BR_T_PROVISIE_REGELING_CODE", CZ_SAS_BR_T_PROVISIE_REGELING_CODE)
    spark.udf.register("CZ_SAS_BR_T_PROVISIE_REGELING_OMS", CZ_SAS_BR_T_PROVISIE_REGELING_OMS)
    spark.udf.register("CZ_SAS_BR_T_PROVISIE_REGELING_PCT", CZ_SAS_BR_T_PROVISIE_REGELING_PCT)
    spark.udf.register("CZ_SAS_BR_T_PROVISIE_RGL_TYPE_CODE", CZ_SAS_BR_T_PROVISIE_RGL_TYPE_CODE)
    spark.udf.register("CZ_SAS_BR_T_PUNT_LEEGMAKEN", CZ_SAS_BR_T_PUNT_LEEGMAKEN)
    spark.udf.register("CZ_SAS_BR_T_PZP_COLLECTIVITEIT_IDC", CZ_SAS_BR_T_PZP_COLLECTIVITEIT_IDC)
    spark.udf.register("CZ_SAS_BR_T_RENDEMENT_LABELORG", CZ_SAS_BR_T_RENDEMENT_LABELORG)
    spark.udf.register("CZ_SAS_BR_T_RENDEMENT_LABELORG_MAAND", CZ_SAS_BR_T_RENDEMENT_LABELORG_MAAND)
    spark.udf.register("CZ_SAS_BR_T_RESTITUTIE_BEDRAG_BRK", CZ_SAS_BR_T_RESTITUTIE_BEDRAG_BRK)
    spark.udf.register("CZ_SAS_BR_T_SELECTIE_DATUM", CZ_SAS_BR_T_SELECTIE_DATUM)
    spark.udf.register("CZ_SAS_BR_T_SOORT_HONORARIUM_KOSTEN", CZ_SAS_BR_T_SOORT_HONORARIUM_KOSTEN)
    spark.udf.register("CZ_SAS_BR_T_SPECIALISME", CZ_SAS_BR_T_SPECIALISME)
    spark.udf.register("CZ_SAS_BR_T_SPECIALISME_DIAGNOSE", CZ_SAS_BR_T_SPECIALISME_DIAGNOSE)
    spark.udf.register("CZ_SAS_BR_T_START_KREDIETTERMIJN", CZ_SAS_BR_T_START_KREDIETTERMIJN)
    spark.udf.register("CZ_SAS_BR_T_STEDELIJKHEID_OMS", CZ_SAS_BR_T_STEDELIJKHEID_OMS)
    spark.udf.register("CZ_SAS_BR_T_STRING_VOORLOOPNULLEN", CZ_SAS_BR_T_STRING_VOORLOOPNULLEN)
    spark.udf.register("CZ_SAS_BR_T_SUBSTR_MUTATIETOTAAL", CZ_SAS_BR_T_SUBSTR_MUTATIETOTAAL)
    spark.udf.register("CZ_SAS_BR_T_TELEFONIE_AANBODTYPE", CZ_SAS_BR_T_TELEFONIE_AANBODTYPE)
    spark.udf.register("CZ_SAS_BR_T_TERMIJN_DAGEN", CZ_SAS_BR_T_TERMIJN_DAGEN)
    spark.udf.register("CZ_SAS_BR_T_UITSLUITEN_VERGOEDING_FNT", CZ_SAS_BR_T_UITSLUITEN_VERGOEDING_FNT)
    spark.udf.register("CZ_SAS_BR_T_UZOVI", CZ_SAS_BR_T_UZOVI)
    spark.udf.register("CZ_SAS_BR_T_UZOVI_AANV_VERZ", CZ_SAS_BR_T_UZOVI_AANV_VERZ)
    spark.udf.register("CZ_SAS_BR_T_UZOVI_OHRA_OVERIG", CZ_SAS_BR_T_UZOVI_OHRA_OVERIG)
    spark.udf.register("CZ_SAS_BR_T_VERANTWOORDING_DTM", CZ_SAS_BR_T_VERANTWOORDING_DTM)
    spark.udf.register("CZ_SAS_BR_T_VERDRAGSVERZEKERDEN", CZ_SAS_BR_T_VERDRAGSVERZEKERDEN)
    spark.udf.register("CZ_SAS_BR_T_VERRICHTING_SUBCODE", CZ_SAS_BR_T_VERRICHTING_SUBCODE)
    spark.udf.register("CZ_SAS_BR_T_VERR_ATL_UKG", CZ_SAS_BR_T_VERR_ATL_UKG)
    spark.udf.register("CZ_SAS_BR_T_VERZEKERINGSBASIS", CZ_SAS_BR_T_VERZEKERINGSBASIS)
    spark.udf.register("CZ_SAS_BR_T_WANBETALER_PROCES_TYPE", CZ_SAS_BR_T_WANBETALER_PROCES_TYPE)
    spark.udf.register("CZ_SAS_BR_T_WANBET_AAN_AFMELDING_DTM", CZ_SAS_BR_T_WANBET_AAN_AFMELDING_DTM)
    spark.udf.register("CZ_SAS_BR_T_WTG_KDE_IDC", CZ_SAS_BR_T_WTG_KDE_IDC)
    spark.udf.register("CZ_SAS_BR_T_WTG_KDE_IDC_NUM", CZ_SAS_BR_T_WTG_KDE_IDC_NUM)
    spark.udf.register("CZ_SAS_BR_T_WTG_OSL_BDG", CZ_SAS_BR_T_WTG_OSL_BDG)
    spark.udf.register("CZ_SAS_BR_T_ZKA_CODE_NAAR_ID", CZ_SAS_BR_T_ZKA_CODE_NAAR_ID)
    spark.udf.register("CZ_SAS_BR_T_ZKA_ID_NAAR_CODE", CZ_SAS_BR_T_ZKA_ID_NAAR_CODE)
    spark.udf.register("CZ_SAS_BR_T_ZKA_ID_NAAR_OMS", CZ_SAS_BR_T_ZKA_ID_NAAR_OMS)
    spark.udf.register("CZ_SAS_BR_T_ZORGKOSTEN_BASIS", CZ_SAS_BR_T_ZORGKOSTEN_BASIS)
    spark.udf.register("CZ_SAS_BR_T_ZORGPRODUCT", CZ_SAS_BR_T_ZORGPRODUCT)
    spark.udf.register("CZ_SAS_BR_T_ZVL_AGB_CODE8", CZ_SAS_BR_T_ZVL_AGB_CODE8)
    spark.udf.register("CZ_SAS_BR_VOORUITBET_OPENSTAAND_BDG", CZ_SAS_BR_VOORUITBET_OPENSTAAND_BDG)
    spark.udf.register("CZ_SAS_BR_V_ADRES", CZ_SAS_BR_V_ADRES)
    spark.udf.register("CZ_SAS_DATUMS", CZ_SAS_DATUMS)
    spark.udf.register("CZ_SAS_DHMS", CZ_SAS_DHMS)
    spark.udf.register("CZ_SAS_EXTRACT_MILLISECONDS", CZ_SAS_EXTRACT_MILLISECONDS)
    spark.udf.register("CZ_SAS_FMT_DDMMYY10", CZ_SAS_FMT_DDMMYY10)
    spark.udf.register("CZ_SAS_FUZZ", CZ_SAS_FUZZ)
    spark.udf.register("CZ_SAS_GET_NL_DAYOFTHEWEEKNAME", CZ_SAS_GET_NL_DAYOFTHEWEEKNAME)
    spark.udf.register("CZ_SAS_GET_NL_MONTHNAME", CZ_SAS_GET_NL_MONTHNAME)
    spark.udf.register("CZ_SAS_HUIDIGE_SITUATIE", CZ_SAS_HUIDIGE_SITUATIE)
    spark.udf.register("CZ_SAS_INPUT_BEST", CZ_SAS_INPUT_BEST)
    spark.udf.register("CZ_SAS_INTCK", CZ_SAS_INTCK)
    spark.udf.register("CZ_SAS_INTNX", CZ_SAS_INTNX)
    spark.udf.register("CZ_SAS_INTNX_DATE", CZ_SAS_INTNX_DATE)
    spark.udf.register("CZ_SAS_INTNX_TS", CZ_SAS_INTNX_TS)
    spark.udf.register("CZ_SAS_ISPARTNER", CZ_SAS_ISPARTNER)
    spark.udf.register("CZ_SAS_KIK_ID", CZ_SAS_KIK_ID)
    spark.udf.register("CZ_SAS_MOD", CZ_SAS_MOD)
    spark.udf.register("CZ_SAS_PUT_Z", CZ_SAS_PUT_Z)
    spark.udf.register("CZ_SAS_RELATIE_DECRYPT", CZ_SAS_RELATIE_DECRYPT)
    spark.udf.register("CZ_SAS_RESOLVE_PATTERN", CZ_SAS_RESOLVE_PATTERN)
    spark.udf.register("CZ_SAS_SCAN", CZ_SAS_SCAN)
    spark.udf.register("CZ_SAS_SETSASVERWIJDERDATUM", CZ_SAS_SETSASVERWIJDERDATUM)
    spark.udf.register("CZ_SAS_TEST", CZ_SAS_TEST)
    spark.udf.register("CZ_SAS_TIMEPART", CZ_SAS_TIMEPART)
    spark.udf.register("CZ_SAS_TR_AANMANING_ZWAARTE_OMS", CZ_SAS_TR_AANMANING_ZWAARTE_OMS)
    spark.udf.register("CZ_SAS_TR_AFBOEKING_REDEN_OMS", CZ_SAS_TR_AFBOEKING_REDEN_OMS)
    spark.udf.register("CZ_SAS_TR_BETAALWIJZE_OMS", CZ_SAS_TR_BETAALWIJZE_OMS)
    spark.udf.register("CZ_SAS_TR_BETALINGSREGELING_STATUS_OMS", CZ_SAS_TR_BETALINGSREGELING_STATUS_OMS)
    spark.udf.register("CZ_SAS_TR_BETALINGSREG_EINDE_REDEN_OMS", CZ_SAS_TR_BETALINGSREG_EINDE_REDEN_OMS)
    spark.udf.register("CZ_SAS_TR_BETALINGSVERKEER_STATUS_OMS", CZ_SAS_TR_BETALINGSVERKEER_STATUS_OMS)
    spark.udf.register("CZ_SAS_TR_BETALINGSVERKEER_SUBTYPE", CZ_SAS_TR_BETALINGSVERKEER_SUBTYPE)
    spark.udf.register("CZ_SAS_TR_BOEKING_STATUS_OMS", CZ_SAS_TR_BOEKING_STATUS_OMS)
    spark.udf.register("CZ_SAS_TR_DC_NOTA_STATUS_OMS", CZ_SAS_TR_DC_NOTA_STATUS_OMS)
    spark.udf.register("CZ_SAS_TR_DEBITEURCREDITEUR_SRT_OMS", CZ_SAS_TR_DEBITEURCREDITEUR_SRT_OMS)
    spark.udf.register("CZ_SAS_TR_DEBITEURCREDITEUR_STATUS_OMS", CZ_SAS_TR_DEBITEURCREDITEUR_STATUS_OMS)
    spark.udf.register("CZ_SAS_TR_INCASSOPROCESSTAP_OMS", CZ_SAS_TR_INCASSOPROCESSTAP_OMS)
    spark.udf.register("CZ_SAS_TR_INCASSO_DOSSIER_STATUS_OMS", CZ_SAS_TR_INCASSO_DOSSIER_STATUS_OMS)
    spark.udf.register("CZ_SAS_TR_INCASSO_FASE_OMS", CZ_SAS_TR_INCASSO_FASE_OMS)
    spark.udf.register("CZ_SAS_TR_KREDIETTERMIJN_CATEGORIE_OMS", CZ_SAS_TR_KREDIETTERMIJN_CATEGORIE_OMS)
    spark.udf.register("CZ_SAS_TR_MARKEER_ONTBREKENDE_REF_ID", CZ_SAS_TR_MARKEER_ONTBREKENDE_REF_ID)
    spark.udf.register("CZ_SAS_TR_MARKEER_REF_ID_NVT", CZ_SAS_TR_MARKEER_REF_ID_NVT)
    spark.udf.register("CZ_SAS_TR_OPHOUDING_SCHADE_OMS", CZ_SAS_TR_OPHOUDING_SCHADE_OMS)
    spark.udf.register("CZ_SAS_TR_OUDERDOM_CATEGORIE_OMS", CZ_SAS_TR_OUDERDOM_CATEGORIE_OMS)
    spark.udf.register("CZ_SAS_TR_SCHULDSANERING_SITUATIE_OMS", CZ_SAS_TR_SCHULDSANERING_SITUATIE_OMS)
    spark.udf.register("CZ_SAS_TR_STORNERING_SOORT_OMS", CZ_SAS_TR_STORNERING_SOORT_OMS)
    spark.udf.register("CZ_SAS_TR_STORNERING_STATUS_OMS", CZ_SAS_TR_STORNERING_STATUS_OMS)
    spark.udf.register("CZ_SAS_TR_SUM", CZ_SAS_TR_SUM)
    spark.udf.register("CZ_SAS_TR_UITVAL_REDEN_OMS", CZ_SAS_TR_UITVAL_REDEN_OMS)
    spark.udf.register("CZ_SAS_TR_VORDERING_STATUS_OMS", CZ_SAS_TR_VORDERING_STATUS_OMS)
    spark.udf.register("CZ_SAS_TR_WANBETALER_AANKOND_TYPE_OMS", CZ_SAS_TR_WANBETALER_AANKOND_TYPE_OMS)
    spark.udf.register("CZ_SAS_TR_WANBETALER_PROCES_TYPE_OMS", CZ_SAS_TR_WANBETALER_PROCES_TYPE_OMS)
    spark.udf.register("CZ_SAS_TR_WANBET_AANAFMELD_REDEN_OMS", CZ_SAS_TR_WANBET_AANAFMELD_REDEN_OMS)
    spark.udf.register("CZ_SAS_TR_WANBET_AANAFMELD_STATUS_OMS", CZ_SAS_TR_WANBET_AANAFMELD_STATUS_OMS)
    spark.udf.register("CZ_SAS_TR_WANBET_AANAFMELD_TYPE_OMS", CZ_SAS_TR_WANBET_AANAFMELD_TYPE_OMS)
    spark.udf.register("CZ_SAS_TR_WERKAANBOD_TYPE_OMS", CZ_SAS_TR_WERKAANBOD_TYPE_OMS)
    spark.udf.register("CZ_SAS_TR_WERKOPDRACHT_STATUS_OMS", CZ_SAS_TR_WERKOPDRACHT_STATUS_OMS)
    spark.udf.register("CZ_SAS_TV_SUBLABEL_NIET_AANWEZIG", CZ_SAS_TV_SUBLABEL_NIET_AANWEZIG)
    spark.udf.register("CZ_SAS_UPPER", CZ_SAS_UPPER)
    spark.udf.register("CZ_SAS_VORIGE_SITUATIE", CZ_SAS_VORIGE_SITUATIE)
    spark.udf.register("CZ_SAS_YYYYMMD", CZ_SAS_YYYYMMD)
    spark.udf.register("CZ_SAS_YYYYMMDD2DATE", CZ_SAS_YYYYMMDD2DATE)
    

    try:
        from prophecy.utils import ScalaUtil
        ScalaUtil.initializeUDFs(spark)
    except :
        pass

@udf(returnType = DateType())
def CZ_SAS_ARIS2SASDATE(date_aris):
    # pylint: disable=import-outside-toplevel
    # pylint: disable=superfluous-parens
    from datetime import date

    try:
        date_str = str(date_aris)
        length = len(date_str)

        # Determine year, month, and day based on length
        if length == 6:  # YYMMDD format
            year, month, day = int(date_str[:2]), int(date_str[2:4]), int(date_str[4:])
            year += 2000 if year < 50 else 1900
        elif length == 8:  # YYYYMMDD format
            year, month, day = int(date_str[:4]), int(date_str[4:6]), int(date_str[6:])
        elif length == 5:  # Edge case like YMMDD (not standard)
            year, month, day = (
int(date_str[0]) + 2000, int(date_str[1:3]), int(date_str[3:]), )
        else:
            return None

        # Validate date ranges directly
        if not (1 <= month <= 12 and 1 <= day <= 31) or not (1583 <= year <= 9999):
            return None

        return date(year, month, day)
    except (ValueError, TypeError):
        return None

def CZ_SAS_BEREKEN_PREMIEGenerator():
    """
from decimal import Decimal, ROUND_HALF_UP 
"""
    from decimal import Decimal, ROUND_HALF_UP # pylint: disable = W0611

    @udf(returnType = DoubleType())
    def func(basisbedrag: float, percentage: float, regelingbedrag: float) -> float:
        basisbedrag = basisbedrag or 0.0
        percentage = percentage or 0.0
        regelingbedrag = regelingbedrag or 0.0
        basisbedrag = Decimal(str(basisbedrag)) # pylint: disable = E0602
        percentage = Decimal(str(percentage)) # pylint: disable = E0602
        regelingbedrag = Decimal(str(regelingbedrag)) # pylint: disable = E0602

        def custom_round(number, pr):
            exp = Decimal("1").scaleb(- pr) # pylint: disable = E0602
            decimal_number = Decimal(str(number)) # pylint: disable = E0602
            rounded_number = decimal_number.quantize(
                exp,
                rounding = ROUND_HALF_UP  # pylint: disable = E0602
            )

            return float(rounded_number)

        rounded_expr = custom_round(
            (
              basisbedrag * percentage / Decimal(str(100.0))  # pylint: disable = E0602
              + regelingbedrag
            ),
            2,
        )
        begrag = basisbedrag + Decimal(str(rounded_expr)) # pylint: disable = E0602

        if begrag < Decimal(str(0.0)):  # pylint: disable = E0602
            return float(0.0)

        return float(begrag)

    return func

CZ_SAS_BEREKEN_PREMIE = CZ_SAS_BEREKEN_PREMIEGenerator()

def CZ_SAS_BEREKEN_REGELINGGenerator():
    """
from decimal import Decimal, ROUND_HALF_UP 
"""
    from decimal import Decimal, ROUND_HALF_UP # pylint: disable = W0611

    @udf(returnType = DoubleType())
    def func(basisbedrag: float, percentage: float, regelingbedrag: float) -> float:
        basisbedrag = basisbedrag or 0.0
        percentage = percentage or 0.0
        regelingbedrag = regelingbedrag or 0.0
        basisbedrag = Decimal(str(basisbedrag)) # pylint: disable = E0602
        percentage = Decimal(str(percentage)) # pylint: disable = E0602
        regelingbedrag = Decimal(str(regelingbedrag)) # pylint: disable = E0602

        def custom_round(number, pr):
            exp = Decimal("1").scaleb(- pr) # pylint: disable = E0602
            decimal_number = Decimal(str(number)) # pylint: disable = E0602
            rounded_number = decimal_number.quantize(
                exp,
                rounding = ROUND_HALF_UP  # pylint: disable = E0602
            )

            return float(rounded_number)

        rounded_expr = custom_round(
            (
              basisbedrag * percentage / Decimal(str(100.0))  # pylint: disable = E0602
              + regelingbedrag
            ),
            2,
        )
        begrag = Decimal(str(rounded_expr)) # pylint: disable = E0602

        if begrag + basisbedrag < Decimal(str(0.0)):  # pylint: disable = E0602
            return float(basisbedrag * Decimal(str(- 1.0))) # pylint: disable = E0602

        return float(begrag)

    return func

CZ_SAS_BEREKEN_REGELING = CZ_SAS_BEREKEN_REGELINGGenerator()

@udf(IntegerType())
def CZ_SAS_BR_BEPAAL_INCPROCESSTAP_CREATIEN():
    return 1

@udf(returnType = IntegerType())
def CZ_SAS_BR_BEPAAL_INCPROCESSTAP_DUMMY():
    return 0

@udf(returnType = IntegerType())
def CZ_SAS_BR_BEPAAL_INCPROCESSTAP_FINTRANS(afboeking_idc, wijziging_nota_idc) -> int:

    if afboeking_idc == 1:
        result = 8
    elif wijziging_nota_idc == 1:
        result = 7
    else:
        result = 99

    return result

@udf(returnType = IntegerType())
def CZ_SAS_BR_BEPAAL_INCPROCESSTAP_ZWAARTE(incasso_fase_code) -> int:

    if incasso_fase_code == 1:
        return 2

    if incasso_fase_code == 2:
        return 3

    if incasso_fase_code == 3:
        return 4

    if incasso_fase_code == 4:
        return 5

    if incasso_fase_code == 5:
        return 6

    return 99

@udf(returnType = DoubleType())
def CZ_SAS_BR_BEREKEN_OPENSTAAND_BEDRAG(
        bedrag,
        bedrag_betaald,
        bedrag_verrekend,
        bedrag_vooruit,
        bedrag_afgeboekt,
        bdg_bet_door_cvz,
) -> float:
    return (
        float(bedrag or 0)
        - float(bedrag_betaald or 0)
        - float(bedrag_verrekend or 0)
        - float(bedrag_vooruit or 0)
        - float(bedrag_afgeboekt or 0)
        - float(bdg_bet_door_cvz or 0)
    )

@udf(returnType = StringType())
def CZ_SAS_BR_BUS_UNIT_2_JE_VZL(business_unit_code: str) -> str:

    def br_bus_unit_2_jur_ent(business_unit_code):

        if business_unit_code is not None:
            if len(business_unit_code) >= 5:
                if business_unit_code[4].isdigit():

                    if business_unit_code[:3] == "CZZ":
                        return "0004"

                    if not business_unit_code[2].isdigit():
                        return ""

                    if business_unit_code[1].isdigit():
                        business_code_part = int(business_unit_code[1:3])

                        return f"{business_code_part:04d}"

                    business_code_part = int(business_unit_code[2:3])

                    return f"{business_code_part:04d}"

        return ""

    if business_unit_code is not None:

        if business_unit_code == "CC411":
            return "0004.12"

        if business_unit_code == "CR852":
            return "0008.21"

        if business_unit_code == "CM852":
            return "0008.24"

        if (
            br_bus_unit_2_jur_ent(business_unit_code) + "." + business_unit_code[
            3:5
        ]
            in (
"0004.11", "0008.52", "0010.11", "0010.52", "0020.11", "0020.52", "0021.11", )
        ):
            return (br_bus_unit_2_jur_ent(business_unit_code) + "." + business_unit_code[3:5])

    return ""

@udf(returnType = StringType())
def CZ_SAS_BR_BUS_UNIT_2_JUR_ENT(business_unit_code: str) -> str:

    if business_unit_code is not None:
        if len(business_unit_code) >= 5:
            if business_unit_code[4].isdigit():

                if business_unit_code[:3] == "CZZ":
                    return "0004"

                if not business_unit_code[2].isdigit():
                    return ""

                if business_unit_code[1].isdigit():
                    business_code_part = int(business_unit_code[1:3])

                    return f"{business_code_part:04d}"

                business_code_part = int(business_unit_code[2:3])

                return f"{business_code_part:04d}"

    return ""

@udf(returnType = DoubleType())
def CZ_SAS_BR_B_OPENST_EXCL_ONINB_BDG(
        bedrag,
        bedrag_betaald,
        bedrag_verrekend,
        bedrag_vooruit,
        bedrag_afgeboekt,
        bdg_bet_door_cvz,
        dc_nota_afb_oninbaar_bdg,
) -> float:
    result = (
        float(bedrag or 0)
        - 1 * float(bedrag_betaald or 0)
        - 1 * float(bedrag_verrekend or 0)
        - 1 * float(bedrag_vooruit or 0)
        - 1 * float(bedrag_afgeboekt or 0)
        - 1 * float(bdg_bet_door_cvz or 0)
        - 1 * float(dc_nota_afb_oninbaar_bdg or 0)
    )

    return result

@udf(returnType = StringType())
def CZ_SAS_BR_CLASSIFY_CHARACTERS(source_str):
    # '# symbol is used as delimiter
    res_string = ""

    if source_str is None:
        return "A"

    for char in source_str:
        if "A" <= char <= "Z":
            res_string += "C#"
        elif "0" <= char <= "9":
            res_string += "N#"
        else:
            res_string += "A#"

    return res_string[:- 1]

@udf(returnType = IntegerType())
def CZ_SAS_BR_COULANCE_SOORT(kostensoort_code) -> int:

    if kostensoort_code == "699100":
        return 1

    if kostensoort_code == "699110":
        return 2

    if kostensoort_code == "699200":
        return 3

    if kostensoort_code == "699300":
        return 4

    return 0

@udf(returnType = DoubleType())
def CZ_SAS_BR_CREEER_INCASSO_FASE_CODE(aanmaning_zwaarte_code: float, dossier_idc: float) -> float:

    if aanmaning_zwaarte_code is None:
        return None

    if aanmaning_zwaarte_code > 5 and dossier_idc == 1:
        return 5.0

    if aanmaning_zwaarte_code > 5 and dossier_idc is None:
        return 4.0

    return aanmaning_zwaarte_code

@udf(returnType = IntegerType())
def CZ_SAS_BR_CREEER_OUDERDOM_CAT_CODE():
    return 0

@udf(returnType = IntegerType())
def CZ_SAS_BR_CVZ_REGELING_IDC(status_nota, bdg_bet_door_cvz) -> int:

    if status_nota == 12 or bdg_bet_door_cvz != 0:
        return 1

    return 0

@udf(returnType = BooleanType())
def CZ_SAS_BR_EXCL_GESPREID_BETALEN_ER(betalingsregeling_type) -> bool:
    return betalingsregeling_type != 2

@udf(returnType = BooleanType())
def CZ_SAS_BR_EXCL_KRUISDOSSIER(status_dossier):
    return status_dossier != 7

@udf(returnType = BooleanType())
def CZ_SAS_BR_EXCL_SOMMATIEDOSSIER(volgnr_sommatie):
    return volgnr_sommatie is None

@udf(returnType = BooleanType())
def CZ_SAS_BR_FIN_DEB_STARTDTM_DM(paramkolom):
    # pylint: disable=import-outside-toplevel
    from datetime import datetime

    if paramkolom is None:
        return False

    compare_date = datetime.strptime("2010-12-01", "%Y-%m-%d").date()

    return paramkolom >= compare_date

@udf(returnType = StringType())
def CZ_SAS_BR_F_AFDELING_MACHTIGING(param):

    # CASE WHEN &MACHTIGING_WORKFLOW_STATUS_NAAM LIKE '%medisch adviseur%'
    # THEN ""47900"" ELSE ""62003A""
    if "medisch adviseur" in param:
        return "47900"

    return "62003A"

@udf(returnType = DoubleType())
def CZ_SAS_BR_F_ATL_BEHANDELINGEN_BOEKING(
        declaratieboeking_soort_id,
        declaratieboeking_bdg,
        prestatiecodenr,
        productgroep_code,
        kostenrubriek_code,
        declaratieboeking_dtm,
        kostensoort_code,
        schaderegel_categorie_id,
        verzekeringgrondslag_code,
        eigen_bijdrage_initieel_bdg,
        prestatie_atl_br,
        boeking_eenheden_atl,
) -> float:

    def signum(value):
        return 1 if value > 0 else - 1 if value < 0 else 0

    if (
        declaratieboeking_soort_id in (2, 6)
        and abs(declaratieboeking_bdg) >= 0.001
        and prestatiecodenr == 1
        and (
          productgroep_code
          in ("DB", "EP", "FP", "FY", "GH", "GP", "HA", "HC", "HE", "HM", "HV", "IB", "MD", "MR", "NG", "OM", "SP", "TA",
             "TC", "VO",)
        )
    ):
        return signum(declaratieboeking_bdg)

    if prestatiecodenr == 60 and abs(declaratieboeking_bdg) >= 0.001:
        return signum(declaratieboeking_bdg)

    if prestatiecodenr == 999 and abs(declaratieboeking_bdg) >= 0.001:
        return signum(declaratieboeking_bdg)

    if kostenrubriek_code == "02" and abs(declaratieboeking_bdg) >= 0.001:
        return signum(declaratieboeking_bdg)

    if (
        declaratieboeking_dtm < datetime.strptime("23MAY22", "%d%b%y")
        and kostensoort_code in ("658000", "568000", "640800")
        and schaderegel_categorie_id in (1, 6, 7)
        and verzekeringgrondslag_code in ("21", "24", "52")
        and eigen_bijdrage_initieel_bdg < 0
    ):
        return prestatie_atl_br

    if (
        declaratieboeking_dtm < datetime.strptime("23MAY22", "%d%b%y")
        and kostensoort_code == "688000"
        and schaderegel_categorie_id in (1, 6, 7)
        and verzekeringgrondslag_code in ("21", "24", "52")
        and eigen_bijdrage_initieel_bdg < 0
    ):
        return prestatie_atl_br / 60

    if (
        declaratieboeking_soort_id == - 2
        or (
          declaratieboeking_soort_id in (2, 6)
          and abs(declaratieboeking_bdg) >= 0.001
        )
    ):
        return boeking_eenheden_atl

    return 0

@udf(returnType = IntegerType())
def CZ_SAS_BR_F_CRMZ_LEEFTIJD_TM() -> int:
    return 65

@udf(returnType = DoubleType())
def CZ_SAS_BR_F_DBC_AANTAL(declaratie_code, behandeling_atl_afleveringen_br):

    if declaratie_code != "":
        if behandeling_atl_afleveringen_br is not None:
            return behandeling_atl_afleveringen_br

    return 0

@udf(returnType = StringType())
def CZ_SAS_BR_F_EMAILEXTBESTAND():
    return "Team 1 - BI <fpa.sens.team1.bi@cz.nl>"

# GDS prestatiecodelijsten GGZ
@udf(returnType = DoubleType())
def CZ_SAS_BR_F_GDS_PRCODELIJST_GGZ() -> float:
    return float(71)

# GDS prestatiecodelijsten Paramedisch
@udf(returnType = StringType())
def CZ_SAS_BR_F_GDS_PRCODELIJST_PARAMEDISCH() -> str:
    return "73,74,75,76,77,78,79,80"

@udf(returnType = IntegerType())
def CZ_SAS_BR_F_GELDIGHEID_STATUS(peildatum, ingang_dtm, eind_dtm):
    # case
    #     when &PEILDATUM BETWEEN &INGANG_DTM AND &EIND_DTM
    #         then 1
    #     when &EIND_DTM < &PEILDATUM
    #         then 2
    #     when &INGANG_DTM > &PEILDATUM
    #         then 3
    #     when &INGANG_DTM = &EIND_DTM
    #         then 4
    #     else .
    # end
    result = None

    if ingang_dtm < peildatum <= eind_dtm:
        result = 1
    elif eind_dtm < peildatum:
        result = 2
    elif ingang_dtm > peildatum:
        result = 3
    elif ingang_dtm == eind_dtm:
        result = 4

    return result

@udf(returnType = StringType())
def CZ_SAS_BR_F_LABEL_VERSTREKKING(label_code, je_vzl_combi_code):

    if label_code in (" ", "!@"):

        if je_vzl_combi_code not in (" ", "!@"):

            if je_vzl_combi_code.startswith("0010"):
                return "004"

            if je_vzl_combi_code.startswith("0020") or je_vzl_combi_code.startswith("0021"):
                return "005"

            return "001"

        return ""

    return label_code

@udf(returnType = IntegerType())
def CZ_SAS_BR_F_LEIDENDE_ZORGVERLENER(instelling_id, praktijk_id, zorgverlener_id, uitschrijver_id) -> int:

    if instelling_id > 0:
        result = instelling_id
    elif praktijk_id > 0:
        result = praktijk_id
    elif zorgverlener_id > 0:
        result = zorgverlener_id
    elif uitschrijver_id > 0:
        result = uitschrijver_id
    else:
        result = - 2

    return result

@udf(returnType = DoubleType())
def CZ_SAS_BR_F_PENSIOENLEEFTIJD_MENV() -> float:
    return float(67)

@udf(returnType = IntegerType())
def CZ_SAS_BR_F_PERSONEEL_COLLECTIVITEIT_ID(overeenkomstnummer_moeder) -> int:

    if overeenkomstnummer_moeder in ("001804529", "001804537"):
        result = 1
    else:
        result = 0

    return result

@udf(returnType = StringType())
def CZ_SAS_BR_F_PRESTATIE_ATL_BOEKING(declaratieboeking_soort_id, declaratieboeking_bdg, prestatie_atl) -> str:

    if (
        declaratieboeking_soort_id == - 2
        or (
          declaratieboeking_soort_id in [2, 6]
          and abs(declaratieboeking_bdg) >= 0.001
        )
    ):
        return prestatie_atl

    return ""

@udf(returnType = StringType())
def CZ_SAS_BR_F_SRT_VERZEKERING_OMS(param):

    if param in ("IR", "VR"):
        return "niet bepaald"

    if param == "NA" or param[1:2] == "N":
        return "natura"

    if param == "RE" or param[1:2] == "R":
        return "restitutie"

    if param == "SE" or param[1:2] == "S":
        return "natura select"

    if param in ("DA", "DB", "DD", "DI", "ED", "DO"):
        return "Just"

    if param in ("CD", "AC", "CA", "CO", "EC"):
        return "CZ-direct"

    if param in ("NC", "OC"):
        return "combinatiepolis"

    return "niet bepaald"

@udf(returnType = IntegerType())
def CZ_SAS_BR_F_UKGRGL_PARTITIONS() -> int:
    return 2013

@udf(returnType = BooleanType())
def CZ_SAS_BR_F_ZKA_DIVISIE_FILTER(divisie_nummer, target="45999"):
    """
    Filters records by checking if the zero-padded 5-digit division number matches the target.

    Parameters:
    divisie_nummer (int): The division number to be checked.
    target (str): The target division code to match, default is "45999".

    Returns:
    bool: True if the zero-padded division number matches the target; False otherwise.
    """
    # Convert the division number to a zero-padded 5-digit string
    divisie_nummer_str = f"{int(divisie_nummer):05}"

    # Check if it matches the target code
    return divisie_nummer_str == target

# Business Rule Function: cz_sas_br_f_zka_sector
@udf(returnType = StringType())
def CZ_SAS_BR_F_ZKA_SECTOR(zorgeenheid_code, zorgeenheid_kort_oms) -> str:

    try:
        zorgeenheid_code_int = int(zorgeenheid_code)
    except (ValueError, TypeError):
        zorgeenheid_code_int = - 1

    zorgeenheid_kort_oms = "".join(ch for ch in zorgeenheid_kort_oms if ch != "D" if ch.isalnum())

    if 0 <= zorgeenheid_code_int < 100:
        return "Alle"

    first_word = zorgeenheid_kort_oms.split(" ", maxsplit = 1)

    if first_word in ("VV", "ZZP"):
        return "VenV"

    if first_word == "GGZ":
        return "GGZ"

    if first_word[:2] in ("LG", "SG", "VG", "ZG", "LV"):
        return "GZ"

    return "Onbekend"

@udf(IntegerType())
def CZ_SAS_BR_IND_ALLEEN_AV(product_code, soort_verzekerden_code):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = soort_verzekerden_code[:2] == "AV"

    return 1 if (condition_1 and condition_2) else 0

@udf(IntegerType())
def CZ_SAS_BR_IND_AV(product_code, soort_verzekerden_code):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = (
        (
          soort_verzekerden_code.rstrip()[:2] in ["AV", "CO"]
          or product_code == "2844"
        )
        and (
          soort_verzekerden_code != "COMBIBU"
          or product_code != "1091"
        )
    )

    return 1 if (condition_1 and condition_2) else 0

@udf(IntegerType())
def CZ_SAS_BR_IND_AV_COLLECTIEF(product_code, soort_verzekerden_code, contractvorm_code):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = (
        (
          soort_verzekerden_code.rstrip()[:2] in ["AV", "CO"]
          or product_code == "2844"
        )
        and (
          soort_verzekerden_code != "COMBIBU"
          or product_code != "1091"
        )
    )
    condition_3 = soort_verzekerden_code not in ["HVV", "COMBIV"]
    condition_4 = soort_verzekerden_code not in ["HVBU", "COMBIBU"]
    condition_5 = contractvorm_code != "IND"

    return (1 if (condition_1 and condition_2 and condition_3 and condition_4 and condition_5) else 0)

@udf(IntegerType())
def CZ_SAS_BR_IND_AV_COLLECTIEF_RECHTSTREEKS(
        product_code,
        soort_verzekerden_code,
        contractvorm_code,
        verkoopkanaal_code
):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = (
        (
          soort_verzekerden_code.rstrip()[:2] in ["AV", "CO"]
          or product_code == "2844"
        )
        and (
          soort_verzekerden_code != "COMBIBU"
          or product_code != "1091"
        )
    )
    condition_3 = soort_verzekerden_code not in ["HVV", "COMBIV"]
    condition_4 = soort_verzekerden_code not in ["HVBU", "COMBIBU"]
    condition_5 = contractvorm_code != "IND"
    condition_6 = verkoopkanaal_code in ["ZAK", "D&V"]

    return (1 if (condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6) else 0)

@udf(IntegerType())
def CZ_SAS_BR_IND_AV_COLLECTIEF_TUSSENPERSOON(
        product_code,
        soort_verzekerden_code,
        contractvorm_code,
        verkoopkanaal_code
):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = (
        (
          soort_verzekerden_code.rstrip()[:2] in ["AV", "CO"]
          or product_code == "2844"
        )
        and (
          soort_verzekerden_code != "COMBIBU"
          or product_code != "1091"
        )
    )
    condition_3 = soort_verzekerden_code not in ["HVV", "COMBIV"]
    condition_4 = soort_verzekerden_code not in ["HVBU", "COMBIBU"]
    condition_5 = contractvorm_code != "IND"
    condition_6 = verkoopkanaal_code not in ["ZAK", "D&V"]

    return (1 if (condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6) else 0)

@udf(IntegerType())
def CZ_SAS_BR_IND_AV_INDIVIDUEEL(product_code, soort_verzekerden_code, contractvorm_code):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = (
        (
          soort_verzekerden_code.rstrip()[:2] in ["AV", "CO"]
          or product_code == "2844"
        )
        and (
          soort_verzekerden_code != "COMBIBU"
          or product_code != "1091"
        )
    )
    condition_3 = soort_verzekerden_code not in ["HVV", "COMBIV"]
    condition_4 = soort_verzekerden_code not in ["HVBU", "COMBIBU"]
    condition_5 = contractvorm_code == "IND"

    return (1 if (condition_1 and condition_2 and condition_3 and condition_4 and condition_5) else 0)

@udf(IntegerType())
def CZ_SAS_BR_IND_AV_INDIVIDUEEL_RECHTSTREEKS(
        product_code,
        soort_verzekerden_code,
        contractvorm_code,
        verkoopkanaal_code
):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = (
        (
          soort_verzekerden_code.rstrip()[:2] in ["AV", "CO"]
          or product_code == "2844"
        )
        and (
          soort_verzekerden_code != "COMBIBU"
          or product_code != "1091"
        )
    )
    condition_3 = soort_verzekerden_code not in ["HVV", "COMBIV"]
    condition_4 = soort_verzekerden_code not in ["HVBU", "COMBIBU"]
    condition_5 = contractvorm_code == "IND"
    condition_6 = verkoopkanaal_code in ["ZAK", "D&V"]

    return (1 if (condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6) else 0)

@udf(IntegerType())
def CZ_SAS_BR_IND_AV_INDIVIDUEEL_TUSSENPERSOON(
        product_code,
        soort_verzekerden_code,
        contractvorm_code,
        verkoopkanaal_code
):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = (
        (
          soort_verzekerden_code.rstrip()[:2] in ["AV", "CO"]
          or product_code == "2844"
        )
        and (
          soort_verzekerden_code != "COMBIBU"
          or product_code != "1091"
        )
    )
    condition_3 = soort_verzekerden_code not in ["HVV", "COMBIV"]
    condition_4 = soort_verzekerden_code not in ["HVBU", "COMBIBU"]
    condition_5 = contractvorm_code == "IND"
    condition_6 = verkoopkanaal_code not in ["ZAK", "D&V"]

    return (1 if (condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6) else 0)

@udf(IntegerType())
def CZ_SAS_BR_IND_AV_PARTICULIER(product_code, soort_verzekerden_code):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = (
        soort_verzekerden_code.rstrip()[:2] in ["AV", "CO"]
        and (
          soort_verzekerden_code != "COMBIBU"
          or product_code != "1091"
        )
    )
    condition_3 = soort_verzekerden_code in ["HVBU", "COMBIBU"]

    return 1 if (condition_1 and condition_2 and condition_3) else 0

@udf(IntegerType())
def CZ_SAS_BR_IND_AV_VERDRAG(product_code, soort_verzekerden_code):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = (
        soort_verzekerden_code.rstrip()[:2] in ["AV", "CO"]
        and (
          soort_verzekerden_code != "COMBIBU"
          or product_code != "1091"
        )
    )
    condition_3 = soort_verzekerden_code in ["HVV", "COMBIV"]

    return 1 if (condition_1 and condition_2 and condition_3) else 0

@udf(IntegerType())
def CZ_SAS_BR_IND_BASIS(product_code, soort_verzekerden_code):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = (
        (soort_verzekerden_code.rstrip()[:2] in ["HV", "CO"])
        or (product_code in ["2840", "2843", "2844", "4567"])
    )

    return 1 if (condition_1 and condition_2) else 0

@udf(IntegerType())
def CZ_SAS_BR_IND_BASIS_PARTICULIER(product_code, soort_verzekerden_code):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = (
        (soort_verzekerden_code.rstrip()[:2] in ["HV", "CO"])
        or (product_code in ["2840", "2843", "2844", "4567"])
    )
    condition_3 = soort_verzekerden_code in ["HVBU", "COMBIBU"]

    return 1 if (condition_1 and condition_2 and condition_3) else 0

@udf(IntegerType())
def CZ_SAS_BR_IND_BASIS_VERDRAG(product_code, soort_verzekerden_code):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = soort_verzekerden_code.rstrip()[:2] in ["HV", "CO"]
    condition_3 = soort_verzekerden_code in ["HVV", "COMBIV"]

    return 1 if (condition_1 and condition_2 and condition_3) else 0

@udf(IntegerType())
def CZ_SAS_BR_IND_COLLECTIEF(product_code, soort_verzekerden_code, contractvorm_code):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = (
        (soort_verzekerden_code.rstrip()[:2] in ["HV", "CO"])
        or (product_code in ["2840", "2843", "2844", "4567"])
    )
    condition_3 = soort_verzekerden_code not in ["HVV", "COMBIV"]
    condition_4 = soort_verzekerden_code not in ["HVBU", "COMBIBU"]
    condition_5 = contractvorm_code != "IND"

    return (1 if (condition_1 and condition_2 and condition_3 and condition_4 and condition_5) else 0)

@udf(IntegerType())
def CZ_SAS_BR_IND_COLLECTIEF_RECHTSTREEKS(
        product_code,
        soort_verzekerden_code,
        contractvorm_code,
        verkoopkanaal_code
):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = (
        (soort_verzekerden_code.rstrip()[:2] in ["HV", "CO"])
        or (product_code in ["2840", "2843", "2844", "4567"])
    )
    condition_3 = soort_verzekerden_code not in ["HVV", "COMBIV"]
    condition_4 = soort_verzekerden_code not in ["HVBU", "COMBIBU"]
    condition_5 = contractvorm_code != "IND"
    condition_6 = verkoopkanaal_code in ["ZAK", "D&V"]

    return (1 if (condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6) else 0)

@udf(IntegerType())
def CZ_SAS_BR_IND_COLLECTIEF_TUSSENPERSOON(
        product_code,
        soort_verzekerden_code,
        contractvorm_code,
        verkoopkanaal_code
):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = (
        (soort_verzekerden_code.rstrip()[:2] in ["HV", "CO"])
        or (product_code in ["2840", "2843", "2844", "4567"])
    )
    condition_3 = soort_verzekerden_code not in ["HVV", "COMBIV"]
    condition_4 = soort_verzekerden_code not in ["HVBU", "COMBIBU"]
    condition_5 = contractvorm_code != "IND"
    condition_6 = verkoopkanaal_code not in ["ZAK", "D&V"]

    return (1 if (condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6) else 0)

@udf(IntegerType())
def CZ_SAS_BR_IND_INDIVIDUEEL(product_code, soort_verzekerden_code, contractvorm_code):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = (
        (soort_verzekerden_code.rstrip()[:2] in ["HV", "CO"])
        or (product_code in ["2840", "2843", "2844", "4567"])
    )
    condition_3 = soort_verzekerden_code not in ["HVV", "COMBIV"]
    condition_4 = soort_verzekerden_code not in ["HVBU", "COMBIBU"]
    condition_5 = contractvorm_code == "IND"

    return (1 if (condition_1 and condition_2 and condition_3 and condition_4 and condition_5) else 0)

@udf(IntegerType())
def CZ_SAS_BR_IND_INDIVIDUEEL_RECHTSTREEKS(
        product_code,
        soort_verzekerden_code,
        contractvorm_code,
        verkoopkanaal_code
):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = (
        (soort_verzekerden_code.rstrip()[:2] in ["HV", "CO"])
        or (product_code in ["2840", "2843", "2844", "4567"])
    )
    condition_3 = soort_verzekerden_code not in ["HVV", "COMBIV"]
    condition_4 = soort_verzekerden_code not in ["HVBU", "COMBIBU"]
    condition_5 = contractvorm_code == "IND"
    condition_6 = verkoopkanaal_code in ["ZAK", "D&V"]

    return (1 if (condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6) else 0)

@udf(IntegerType())
def CZ_SAS_BR_IND_INDIVIDUEEL_TUSSENPERSOON(
        product_code,
        soort_verzekerden_code,
        contractvorm_code,
        verkoopkanaal_code
):
    condition_1 = product_code not in ["1147", "1192", "1343"]
    condition_2 = (
        (soort_verzekerden_code.rstrip()[:2] in ["HV", "CO"])
        or (product_code in ["2840", "2843", "2844", "4567"])
    )
    condition_3 = soort_verzekerden_code not in ["HVV", "COMBIV"]
    condition_4 = soort_verzekerden_code not in ["HVBU", "COMBIBU"]
    condition_5 = contractvorm_code == "IND"
    condition_6 = verkoopkanaal_code not in ["ZAK", "D&V"]

    return (1 if (condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6) else 0)

@udf(IntegerType())
def CZ_SAS_BR_IND_JONGER_DAN_18(param_peildatum, sas_dtm_gbt_rlt, soort_verzekerden_code, is_gevangen):
    # pylint: disable=import-outside-toplevel
    from datetime import date, timedelta
    from dateutil.relativedelta import relativedelta
    import calendar

    # this INTNX code was taken from CZ_SAS_INTNX_DATE UDF
    def intnx(custom_interval, start_from, increment, alignment="BEGINNING"):
        custom_interval = custom_interval.upper()
        alignment = alignment.upper()

        if not isinstance(start_from, date):
            start_from = date(start_from.year, start_from.month, start_from.day)

        if alignment in ("S", "SAME", "SAMEDAY"):

            if custom_interval in ("DAY", "DAYS", "DTDAY"):
                return start_from + timedelta(days = increment)

            if custom_interval in ("WEEK", "DTWEEK"):
                return start_from + timedelta(weeks = increment)

            if custom_interval in ("MONTH", "MONTHS", "DTMONTH"):
                return date(start_from.year, start_from.month, start_from.day) + relativedelta(months = increment)

            if custom_interval in ("QTR", "DTQTR"):
                return date(start_from.year, start_from.month, start_from.day) + relativedelta(months = increment * 3)

            if custom_interval in ("YEAR", "YEARS", "DTYEAR"):
                # return start_from + relativedelta(years=increment)
                return date(start_from.year, start_from.month, start_from.day) + relativedelta(years = increment)

            raise ValueError(f"{custom_interval} interval is not currently supported")

        if alignment in ("BEGINNING", "B"):

            if custom_interval in ("DAY", "DAYS", "DTDAY"):
                return start_from + timedelta(days = increment)

            if custom_interval in ("WEEK", "DTWEEK"):
                return (start_from + timedelta(weeks = increment) - timedelta(days = start_from.weekday() + 1))

            if custom_interval in ("MONTH", "MONTHS", "DTMONTH"):
                res_date = date(start_from.year, start_from.month, start_from.day) + relativedelta(months = increment)

                return date(res_date.year, res_date.month, 1)

            if custom_interval in ("YEAR", "YEARS", "DTYEAR"):
                start_from = start_from.replace(month = 1).replace(day = 1)

                return start_from + relativedelta(years = increment)

            raise ValueError(f"{custom_interval} interval is not currently supported")

        if alignment in ("END", "E"):

            if custom_interval in ("DAY", "DAYS", "DTDAY"):
                return start_from + timedelta(days = increment)

            if custom_interval in ("WEEK", "DTWEEK"):
                return (
                    start_from
                    + timedelta(weeks = increment)
                    - timedelta(days = start_from.weekday())
                    + timedelta(days = 5)
                )

            if custom_interval in ("MONTH", "MONTHS", "DTMONTH"):
                res_date = start_from + relativedelta(months = increment)

                return date(res_date.year, res_date.month, calendar.monthrange(res_date.year, res_date.month)[1], )

            if custom_interval in ("YEAR", "YEARS", "DTYEAR"):
                res_date = date(start_from.year, start_from.month, start_from.day) + relativedelta(years = increment)

                return date(res_date.year, 12, res_date.day)

            raise ValueError(f"{custom_interval} interval is not currently supported")

        # end of INTNX code
        raise ValueError(f"{alignment} alignment is not currently supported")

    if sas_dtm_gbt_rlt is None:
        return 0

    condition_1 = intnx("year", param_peildatum, - 18, "s") <= sas_dtm_gbt_rlt
    condition_2 = is_gevangen == 1 or soort_verzekerden_code in ("HVB", "COMBIB")

    return 1 if (condition_1 and condition_2) else 0

@udf(IntegerType())
def CZ_SAS_BR_IND_VERDRAG_GEEN_RECHT(product_code):
    condition_1 = product_code == "1147"

    return 1 if (condition_1) else 0

@udf(returnType = StringType())
def CZ_SAS_BR_REMOVE_CONTROL_CHARACTERS(s):
    # pylint: disable=import-outside-toplevel
    import unicodedata

    if s is None:
        return None

    result = "".join(ch for ch in s if unicodedata.category(ch)[0] != "C")

    if result == "":
        return None

    return result

@udf(returnType = StringType())
def CZ_SAS_BR_REMOVE_DOUBLE_OTHER_CHARACTERS(source_str):
    res_string = ""

    if source_str is None:
        return None

    for char in source_str:
        if ("A" <= char <= "Z") or ("0" <= char <= "9"):
            res_string += str(char)
        else:
            res_string += chr(26)

    return res_string

@udf(returnType = ArrayType(DoubleType()))
def CZ_SAS_BR_SELECT_BET_REG_EINDE():
    return [2.0, 5.0]

@udf(returnType = IntegerType())
def CZ_SAS_BR_SELECT_BET_REG_START():
    return 1

# pylint: disable=unused-argument
@udf(returnType = StringType())
def CZ_SAS_BR_SEPA(bic, iban, bban):

    if iban is None:
        return bban

    return ((
        iban[0:4]
        + " "
        + iban[4:8]
        + " "
        + iban[8:12]
        + " "
        + iban[12:16]
        + " "
        + iban[16:20]
        + " "
        + iban[20:24]
        + " "
        + iban[24:28]
        + " "
        + iban[28:32]
        + " "
        + iban[32:36]
    )\
        .upper()\
        .strip())

@udf(returnType = StringType())
def CZ_SAS_BR_SUBSTITUTE_BETWEEN_DOUBLE_N(source_str, pattern):
    res_string = ""

    if source_str is None:
        return None

    if pattern[0] == "A":
        res_string += chr(26)
    else:
        res_string += source_str[0]

    if len(source_str) > 1:

        for j in range(1, len(source_str) - 1):
            if (not ((pattern[j - 1] == "N") and (pattern[j + 1] == "N")) and pattern[j] == "A"):
                res_string += chr(26)
            else:
                res_string += source_str[j]

        if pattern[len(source_str) - 1] == "A":
            res_string += chr(26)
        else:
            res_string += source_str[len(source_str) - 1]

    return res_string

@udf(returnType = StringType())
def CZ_SAS_BR_SUBSTITUTE_DOUBLE_A(source_str, pattern):

    if source_str is None:
        return None

    res_string = source_str[0]

    for j in range(1, len(source_str)):
        if pattern[j - 1] == "A" and pattern[j] == "A":
            res_string += chr(26)
        else:
            res_string += source_str[j]

    return res_string

@udf(returnType = StringType())
def CZ_SAS_BR_S_DEBITEURENBEHEER(afdeling_code) -> str:

    if afdeling_code == "60700":
        return "60700"

    return afdeling_code

# paramkolom    S_DEB_START_FIN_MUT    Technical
# &paramkolom. >= '01JAN2014'd
# Controleer of de datum in de kolom groter of gelijk is aan 1 januari 2014
@udf(returnType = BooleanType())
def CZ_SAS_BR_S_DEB_START_FIN_MUT(paramkolom) -> bool:
    if paramkolom is not None:
        try:
            compare_date = datetime.strptime("01JAN2014", "%d%b%Y")
            param_date = datetime.strptime(paramkolom, "%d%b%Y")

            return param_date >= compare_date
        except ValueError:
            return False # Invalid date format
    else:
        return False # None value

@udf(returnType = BooleanType())
def CZ_SAS_BR_S_EXCLHUIDIGEMAAND(paramkolom, syspeildatum) -> bool:
    """UDF"""
    # pylint: disable=import-outside-toplevel
    syspeildatum_month = syspeildatum.strftime("%Y%m")

    if str(paramkolom) != syspeildatum_month:
        return True

    return False

@udf(returnType = BooleanType())
def CZ_SAS_BR_S_FINTRANSSOORT_OPBOEKING(financiele_transactie_srt_code) -> bool:
    return financiele_transactie_srt_code not in (11, 59, 60, 71, 72)

@udf(returnType = IntegerType())
def CZ_SAS_BR_S_FIN_TRANS_OPBOEKING(paramkolom) -> int:

    if paramkolom in (11, 59, 60, 71, 72):
        result = 1
    else:
        result = 0

    return result

@udf(returnType = IntegerType())
def CZ_SAS_BR_S_FIN_TRANS_STORNERING(value) -> int:

    if value in (13, 19):
        result = 1
    else:
        result = 0

    return result

@udf(returnType = IntegerType())
def CZ_SAS_BR_S_GENORM_WERKAANBOD(werkaanbod_type_code, werkaanbod_soort_code) -> int:

    if (
        werkaanbod_type_code == 4
        and (
          werkaanbod_soort_code
          in (
"14", "15", "21", "27", "28", "29", "30", "31", "32", "36", "37", "38", "40", "41", "45", )
        )
    ):
        result = 1
    elif werkaanbod_type_code != 4:
        result = 1
    else:
        result = 0

    return result

@udf(returnType = IntegerType())
def CZ_SAS_BR_S_GENORM_WERKAANB_EXCLMATCH(werkaanbod_type_code, werkaanbod_soort_code, aantal):

    if (
        werkaanbod_type_code == 4
        and (
          werkaanbod_soort_code
          in {
"14", "21", "27", "28", "29", "30", "31", "32", "36", "38", "40", "41", }
    )
    ):
        return aantal

    if werkaanbod_type_code != 4:
        return aantal

    return 0

@udf(returnType = BooleanType())
def CZ_SAS_BR_S_HERHAALVERKEER(klantcontact_soort_column):
    return (
        klantcontact_soort_column == "Balie bezoek"
        or klantcontact_soort_column == "Onbekend"
        or "Inkomend" in klantcontact_soort_column
    )

@udf(returnType = BooleanType())
def CZ_SAS_BR_S_NOTAS_HERBINCASSOKST(afboeking_reden_code) -> bool:

    # check if afboeking_reden_code is not equal to 28
    if afboeking_reden_code != 28:
        return True

    return False

@udf(returnType = BooleanType())
def CZ_SAS_BR_S_RUBRIEKENBESTAND(column, value):
    return column == value

@udf(returnType = DoubleType())
def CZ_SAS_BR_S_TELEFONIE_CENTRAALNR():
    return 9000.0

@udf(returnType = BooleanType())
def CZ_SAS_BR_S_TM_VORIGJAAR(datum, jaar, today) -> bool:
    # pylint: disable=import-outside-toplevel
    # from datetime import date
    current_year_minus_one = str(today.year - 1)

    # today = date.today()
    return (current_year_minus_one <= jaar) and ((datum is None) or (datum <= today))

@udf(returnType = DoubleType())
# case when SUBLABEL = 0 then . when SUBLABEL ^= 0 then SUBLABEL end
def CZ_SAS_BR_TV_SUBLABEL_NIET_AANWEZIG(sublabel):
    result = None

    if sublabel == 0:
        result = None
    elif sublabel != 0:
        result = sublabel

    return result

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_AANTAL_DBC(
        uitvoerder_soort_code,
        verrichting_soort_code,
        verrichting_stelsel_code,
        externe_code,
        uitkeringsregel_invoer_aantal,
):
    condition1 = (
        uitvoerder_soort_code != "03"
        and verrichting_soort_code == "51"
        and verrichting_stelsel_code == "2"
        and externe_code != ""
    )
    condition2 = (
        uitvoerder_soort_code != "03"
        and verrichting_soort_code == "54"
        and verrichting_stelsel_code == "1"
        and externe_code != ""
    )
    condition3 = (verrichting_soort_code == "53" and verrichting_stelsel_code == "2" and externe_code != "")

    if condition1 or condition2 or condition3:
        return uitkeringsregel_invoer_aantal

    return 0

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_AANTAL_DDD(uitkeringsregel_invoer_aantal, daily_defined_doses):

    if uitkeringsregel_invoer_aantal is None or daily_defined_doses is None:
        return None

    return float(uitkeringsregel_invoer_aantal) * float(daily_defined_doses)

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_AANTAL_UREN(
        onderdeel_specifiek_code,
        behandeling_atl_afleveringen_br,
        prestatiecodelijst_nmr,
        prestatie_code,
) -> float:
    prestations_65 = ["1000", "1001", "1002", "1003", "1004", "1005", "1006", "1008", "1009", "1010", "1011", "1012",
                      "1013", "1014", "1017", "1018", "1019", "1023", "1024", "1025", "1026",
                      "1031", "1032", "1033", "1034", "1035", "1036", "1037", "1038", "1041",
                      "1049", "1062", "1110", "1111", "1112", "1113", "1114", "1115", "1118",]
    prestations_69 = ["1008", "1011"]
    res = (0.0 if behandeling_atl_afleveringen_br is None else float(behandeling_atl_afleveringen_br))

    if onderdeel_specifiek_code == "KZU":
        return res

    if prestatiecodelijst_nmr == 65 and prestatie_code in prestations_65:
        return res / 12

    if prestatiecodelijst_nmr == 69 and prestatie_code in prestations_69:
        return res

    return 0.0

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_AANTAL_UREN_GEBOORTEZORG(kostensoort_onderdeel_spec_code, aantal_vrt_br):

    if kostensoort_onderdeel_spec_code == "010000":
        result = aantal_vrt_br / 4
    else:
        result = 0

    return int(result)

# verrichting_subsoort_code: String field containing codes
# AANTAL_VRT_BR: Numeric field with the number of hours
@udf(returnType = DoubleType())
def CZ_SAS_BR_T_AANTAL_UREN_WIJKVERPLEGING(verrichting_subsoort_code, aantal_vrt_br):
    valid_codes = ["1.65.000000001013", "1.65.000000001016", "1.65.000000001022", "1.65.000000001023",
                   "1.65.000000001024", "1.65.000000001025", "1.65.000000001027",
                   "1.65.000000001028", "1.65.000000001029", "1.65.000000001030",]

    if verrichting_subsoort_code in valid_codes:
        return None

    return aantal_vrt_br / 12 if aantal_vrt_br is not None else None

@udf(returnType = StringType())
def CZ_SAS_BR_T_ACTUALITEIT_VERZEKERING(ingangsdatum, einddatum, syspeildatum):

    if ingangsdatum <= syspeildatum and (einddatum is None or einddatum > syspeildatum):
        result = "A" # ACTUEEL
    elif ingangsdatum > syspeildatum:
        result = "T" # TOEKOMST
    elif einddatum is not None and einddatum <= syspeildatum:
        result = "B" # BEEINDIGD
    else:
        result = "E" # Error

    return result

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_ADRES_STATUS(postcode, straatnaam, land_code, adres_status_code, versleuteld_idc):
    result = 5.0
    postcode = postcode.strip() if postcode else ""
    straatnaam = straatnaam.strip() if straatnaam else ""
    land_code = land_code.strip() if land_code else ""

    if postcode.strip() == "" and "onbekend" in straatnaam.lower():
        result = 1.0
    elif (postcode.strip() == "" and land_code.strip() == "NL" and "geheim" in straatnaam.lower()):
        result = 4.0
    elif versleuteld_idc and versleuteld_idc == 1.0:
        result = 6.0
    elif adres_status_code and adres_status_code == 2.0:
        result = 2.0
    else:
        result = 5.0

    return result

# =======================
# == Original SQL code ==
# =======================
@udf(returnType = StringType())
def CZ_SAS_BR_T_ADRES_TYPE(resource_item, adres_type):
    resource_mapping = {
        "STG_EDWH.STG_MAR_Z430900001_HST": "1",  # Woon
        "STG_EDWH.STG_MAR_Z430910001_HST": "2",  # Correspondentie
        "STG_EDWH.STG_MAR_Z430915001_HST": "6",  # Woonlandhistorie
        "STG_EDWH.STG_KIK_PROSPECTADRES_HST": {
          "W": "1",  # Woon
          "C": "2",  # Correspondentie
          "default": "3",  # Onbekend
        },
        "STG_EDWH.STG_MAR_Z406007001_HST": {
          "J": "5",  # Hoofdvestiging
          "default": "4",  # Vestiging
        },
        "STG_EDWH.STG_MAR_Z406009001_HST": adres_type,  # obv KDE_SRT_ADS_ZVL
        "STG_EDWH.STG_MAR_Z405002001_HST": adres_type,  # obv KDE_SRT_ACL
        "default": "3",  # Onbekend
    }
    result = resource_mapping.get(resource_item, resource_mapping["default"])

    if isinstance(result, dict):
        result = result.get(adres_type, result["default"])

    return result

@udf(DoubleType())
def CZ_SAS_BR_T_AFSLUITPROVISIE(
        recht_op_afsluitprovisie_idc,
        provisie_rgl_type_code,
        provisie_regeling_bdg,
        provisie_premievrije_verz_idc,
        netto_premie_bdg,
        provisie_regeling_pct,
):
    provisie_regeling_bdg = (provisie_regeling_bdg if provisie_regeling_bdg is not None else 0.0)
    netto_premie_bdg = netto_premie_bdg if netto_premie_bdg is not None else 0.0
    provisie_regeling_pct = (provisie_regeling_pct if provisie_regeling_pct is not None else 0.0)

    if (
        recht_op_afsluitprovisie_idc == 1
        and provisie_rgl_type_code == "AP"
        and provisie_regeling_bdg > 0
        and (
          provisie_premievrije_verz_idc == 1
          or netto_premie_bdg > 0.005
        )
    ):
        return provisie_regeling_bdg

    if (
        recht_op_afsluitprovisie_idc == 1
        and provisie_rgl_type_code == "AP"
        and provisie_regeling_pct > 0
        and (
          provisie_premievrije_verz_idc == 1
          or netto_premie_bdg > 0.005
        )
    ):
        return (provisie_regeling_pct / 100) * netto_premie_bdg

    return 0.0

@udf(returnType = StringType())
def CZ_SAS_BR_T_ANVA_POLISKEY(relnr, volg_sub):
    formatted_relnr = f"{relnr:08d}" if relnr is not None else "00000000"
    formatted_volg_sub = f"{volg_sub:05d}" if volg_sub is not None else "00000"
    result = formatted_relnr + formatted_volg_sub

    return result

@udf(returnType = LongType())
def CZ_SAS_BR_T_ANVA_SCHADEKEY(schadenr, schadesubnr) -> int:
    v1 = 0 if schadenr is None else schadenr
    v2 = 0 if schadesubnr is None else schadesubnr

    return int(v1) * 100 + int(v2)

@udf(returnType = TimestampType())
def CZ_SAS_BR_T_ANYDT(datum: str):
    # pylint: disable=import-outside-toplevel
    from datetime import datetime
    # datum = "01-01-0001" if not datum or datum.strip() == '' else datum.strip()
    result = None

    try:
        datum = datum.strip()

        if len(datum) > 19:
            datum = datum[:19].strip()

        if len(datum) > 10:
            result = datetime.strptime(datum, "%d-%m-%Y %H:%M:%S")
        else:
            result = datetime.strptime(datum, "%d-%m-%Y")
    except Exception as e:
        print(f"An exception occurred: {str(e)}")
        result = None

    return result

# Business rule to replace "<LEEG>" with "oi" in the given string field
# (ARTNR_VERGOEDING)
@udf(returnType = StringType())
def CZ_SAS_BR_T_ARTNR_VERGOEDING_04(column_value) -> str:

    if column_value is not None and "<LEEG>" in column_value:
        result = column_value.replace("<LEEG>", "oi")
    else:
        result = column_value

    return result

@udf(returnType = StringType())
def CZ_SAS_BR_T_ARTNR_VERGOEDING_07(column) -> str:
    pattern = r"D .15|D .16|D .17|D .18|B .08|B .09|B .10|B .11"

    if re.match(pattern, column, re.IGNORECASE):
        return column

    return ""

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_BDG_BTW_TRF(bdg_trf_nto, trf_mnl) -> float:
    result = (
        (
          (0 if bdg_trf_nto is None else bdg_trf_nto)
          / (0 if trf_mnl is None else trf_mnl + 100)
        )
        * (0 if trf_mnl is None else trf_mnl)
    )

    return result

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_BDG_EB0_NTR(
        brontabel,
        bdg_eb0_ntr,
        sts_ntr,
        ieder_sts_ukg_is_3_idc,
        bdg_ghr_zvl_usv,
        bdg_trf_nto,
        som_ukgrgl_uitvoer_bdg,
        bdg_egr_ntr,
):

    if brontabel not in [
"STG_EDWH.STG_MAR_Z479803001_HST", "STG_EDWH.STG_MAR_Z479703001_HST", ]:
        result = bdg_eb0_ntr
    elif sts_ntr == "3":
        result = 0.0
    elif ieder_sts_ukg_is_3_idc == 1:
        result = - 1.0 * bdg_ghr_zvl_usv
    else:
        result = - 1.0 * (bdg_trf_nto - som_ukgrgl_uitvoer_bdg + bdg_egr_ntr)

    return result

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_BDG_RST_TEE(bdg_trf_nto, bdg_wtg_osl, bdg_btw_trf) -> float:
    bdg_trf_nto_val = 0.0 if bdg_trf_nto is None else float(bdg_trf_nto)
    bdg_wtg_osl_val = 0.0 if bdg_wtg_osl is None else float(bdg_wtg_osl)
    bdg_btw_trf_val = 0.0 if bdg_btw_trf is None else float(bdg_btw_trf)
    result = bdg_trf_nto_val - bdg_wtg_osl_val - bdg_btw_trf_val

    return result

@udf(returnType = BooleanType())
def CZ_SAS_BR_T_BEPAAL_KARAKTER_TYPE():
    return True

@udf(returnType = StringType())
def CZ_SAS_BR_T_BEPAAL_VERZEKERAAR(kde_osl_vza, kde_osl_vza_hdi) -> str:

    if kde_osl_vza == kde_osl_vza_hdi:
        return ""

    return kde_osl_vza

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_BEPAAL_ZORGVERLENER_ID(
        schade_productiestraat_code,
        zorgverlener_instelling_id,
        verrichting_subsoort_code,
        zorgverlener_declarant_id,
        zorgverlener_uitschrijver_id,
) -> int:

    if (
        schade_productiestraat_code == "1"
        and zorgverlener_instelling_id > 0
        and verrichting_subsoort_code[2:4] not in ("53", "94", "95")
    ):
        return zorgverlener_instelling_id

    if schade_productiestraat_code == "3":
        return zorgverlener_declarant_id

    return zorgverlener_uitschrijver_id

@udf(returnType = StringType())
def CZ_SAS_BR_T_BETAALTERMIJN(betaaltermijn_code):

    if betaaltermijn_code == "J":
        return "12"

    if betaaltermijn_code == "H":
        return "06"

    if betaaltermijn_code == "K":
        return "03"

    if betaaltermijn_code == "M":
        return "01"

    return None

@udf(returnType = StringType())
def CZ_SAS_BR_T_BGK_ACTIVITEIT(activiteit):

    if "relatienummer nieuw" in activiteit:
        result = "relatienummer nieuw"
    elif "relatienummer oud" in activiteit:
        result = "relatienummer oud"
    else:
        result = activiteit

    return result

@udf(returnType = StringType())
def CZ_SAS_BR_T_BGK_TYPE(type_column):
    type_str = type_column.lower() if type_column is not None else ""

    if "bezwaar" in type_str:
        return "Bezwaarschriften"

    if "geschil" in type_str:
        return "Geschillen"

    if "klachten" in type_str:
        return "Klachten"

    return "Onbekend"

@udf(returnType = StringType())
def CZ_SAS_BR_T_BSN(bsn):
    bsn = "" if not bsn else bsn

    try:

        if bsn in {"000000000", "999999999", ""}:
            return None

        return str(int(bsn)).zfill(9)
    except Exception as e:
        print(f"An exception occurred: {str(e)}")

        return bsn

@udf(returnType = StringType())
def CZ_SAS_BR_T_CATX(dlm, items):

    def resolve_pattern(
            items,
            pattern,
            dlm,
            token_string = "#"
    ):
        # pylint: disable=import-outside-toplevel
        import re

        try:

            # Check if either items or pattern is empty - if not: provide a
            # description for the function
            if not items or not pattern:
                print("\nRESOLVEPATTERN> Generates text by substituting values of a list into a pattern")
                print("        ")
                print("   Syntax: resolvePattern(item_list, pattern, delimiter, token_string)")
                print("        - item_list:      Item list")
                print("        - delimiter:      List delimiter")
                print("        - pattern:        Pattern for substitution")
                print("        - token_string:   Token for substitution")
                print()

                return ""

            # Convert item_list to a string representation
            if isinstance(items, list):
                item_list_str = " ".join(str(item) if item is not None else "" for item in items)
            else:
                item_list_str = items

            # Split item_list using delimiter
            list_dlm = (item_list_str.split("/")[1].strip() if "/" in item_list_str else " ")
            item_list_str = " ".join(item_list_str.split("/")[0].split())
            # Remove leading and trailing spaces from delimiter and
            # token_string
            dlm = dlm.strip()
            token_string = token_string.strip()
            # If delimiter is empty, set it to a single space
            dlm = " " if not dlm else dlm
            # Substitute tokens in pattern with items
            resolved_pattern = ""

            for item in item_list_str.split(list_dlm):
                if item:

                    if resolved_pattern:
                        resolved_pattern += dlm

                    resolved_pattern += re.sub(re.escape(token_string), item, pattern)

            return resolved_pattern
        except Exception as e:
            print("An error occurred:", str(e))

            return ""

    if dlm == "":
        return "".join(resolve_pattern(
            items,
            "#",
            ","
        ))

    return resolve_pattern(
        items,
        "#",
        dlm
    )

@udf(returnType = BooleanType())
def CZ_SAS_BR_T_CAT_INSTELLING(kde_tbl_tbs, std_tbs):
    # Source condition
    # "KDE_TBL_TBS = '022'
    # AND NOT (substr(STD_TBS,1,7) = '001858A'
    # AND substr(STD_TBS,8,1) not in ('G', 'H', 'I'))
    # AND substr(STD_TBS,1,7) IN  ( '001670M', '001670N', '001686J', '001686K', '001858A', '002737M', '002737N',
    # '002737O', '002737P', '002737Q', '002737R', '002737S', '002737T', '002737U', '002737V',
    # '002737W', '002737Y', '002737Z', '001686M')
    return (
        kde_tbl_tbs == "022"
        and (
          (
            ("001858A" in std_tbs[0:7])
            and std_tbs[7:8] in ["G", "H", "I"]
          )
          or (
            std_tbs[0:7]
            in ["001670M", "001670N", "001686J", "001686K", "002737M", "002737N", "002737O", "002737P", "002737Q",
               "002737R", "002737S", "002737T", "002737U", "002737V", "002737W", "002737Y", "002737Z",
               "001686M",]
          )
        )
    )

# pylint: disable=unused-argument
@udf(DoubleType())
def CZ_SAS_BR_T_CONTINUITEITSPROVISIE(
        provisie_rgl_type_code,
        provisie_premievrije_verz_idc,
        provisie_regeling_pct,
        netto_premie_bdg,
        ingang_dtm,
        eind_dtm,
        jaar,
        provisie_regeling_bdg,
):
    # pylint: disable=import-outside-toplevel
    from datetime import timedelta

    def calculate_days_in_month(date):
        next_month = date.replace(day = 28) + timedelta(days = 4)

        return (next_month - timedelta(days = next_month.day)).day

    def intck_month(start_date, end_date):
        return ((end_date.year - start_date.year) * 12 + end_date.month - start_date.month)

    if (
        provisie_rgl_type_code == "CP"
        and provisie_regeling_pct > 0.0
        and (
          provisie_premievrije_verz_idc == 1.0
          or netto_premie_bdg > 0.005
        )
    ):
        months_between = intck_month(ingang_dtm, eind_dtm)
        start_month_days = calculate_days_in_month(ingang_dtm)
        end_month_days = calculate_days_in_month(eind_dtm)
        partial_months = (
            ((start_month_days - (ingang_dtm.day - 1)) / start_month_days)
            - ((end_month_days - eind_dtm.day) / end_month_days)
        )

        return float((provisie_regeling_pct / 100) * netto_premie_bdg * (months_between + partial_months))

    if (
        provisie_rgl_type_code == "CP"
        and provisie_regeling_bdg > 0.0
        and (
          provisie_premievrije_verz_idc == 1.0
          or netto_premie_bdg > 0.005
        )
    ):
        months_between = intck_month(ingang_dtm, eind_dtm)
        start_month_days = calculate_days_in_month(ingang_dtm)
        end_month_days = calculate_days_in_month(eind_dtm)
        partial_months = (
            ((start_month_days - (ingang_dtm.day - 1)) / start_month_days)
            - ((end_month_days - eind_dtm.day) / end_month_days)
        )

        return float(provisie_regeling_bdg * (months_between + partial_months))

    return 0.0

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_CORRECTIE_NTA(brontabel, kde_osn_nta, acn_nta_tbg, acn_nta_krt, acn_nta_obk) -> int:

    if (brontabel == "STG_EDWH.STG_MAR_Z476802001_HST" and kde_osn_nta != "01" and acn_nta_tbg is None):
        result = 0

    if acn_nta_krt is not None or acn_nta_obk is not None or acn_nta_tbg is not None:
        result = 1
    else:
        result = 0

    return result

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_CORRECTIE_NTR(edwh_resource_code, notaregel_archief_nmr_origineel, notaregel_archief_nmr) -> int:

    if edwh_resource_code == "STG_EDWH.STG_MAR_Z476803001_HST":
        return - 3

    if notaregel_archief_nmr_origineel == "":
        return 0

    if notaregel_archief_nmr_origineel == notaregel_archief_nmr:
        return 0

    return 1

@udf(returnType = StringType())
def CZ_SAS_BR_T_CRMZ_PEILDATUM_VNESTAND(type_, syspeildatum):

    if syspeildatum is None or not syspeildatum.strip():
        return "01JAN1800"

    try:
        syspeildatum_dt = datetime.strptime(syspeildatum.strip(), "%d%b%Y")
    except ValueError:
        # Assuming 'syspeildatum' is in the correct format 'DDMMMYYYY', if not,
        # return default value
        return "01JAN1800"

    type_upper = type_.upper() if type_ else ""

    if type_upper == "VORIG":
        result_date = syspeildatum_dt.replace(day = 31, month = 12, year = syspeildatum_dt.year - 1)
    elif type_upper == "HUIDIG":
        result_date = syspeildatum_dt.replace(day = 31, month = 12)
    elif type_upper == "VOLGEND":
        result_date = syspeildatum_dt.replace(day = 1, month = 1, year = syspeildatum_dt.year + 1)
    else:
        return "01JAN1800"

    result_date_str = result_date.strftime("%d%b%Y").upper()

    return result_date_str

@udf(returnType = StringType())
def CZ_SAS_BR_T_CRMZ_PEILDATUM_VRZSTAND():
    return ""

@udf(returnType = StringType())
def CZ_SAS_BR_T_CZA_PERSONEELSNMR(domainname):

    if domainname is None or len(domainname) < 7:
        return None

    return domainname[6:12]

@udf(returnType = DateType())
def CZ_SAS_BR_T_DATUM(param1, param2):
    # this function evaluates the following code in SAS environment:
    # COALESCE(INPUT(&&param1., ANYDTDTE.),INPUT(&&param2.,ANYDTDTE.))
    # The minimum date supported by ANYDTDTE informat in SAS is January 1, 1582.
    # This is consistent with the minimum date allowed in the Gregorian calendar,
    # which is the standard calendar system used in most of the world today.
    # For input '00090101', it must return the date '2009-Jan-01', as shows the DTR_MAR_Z405001001 job run.
    # Function br_t_datum uses ANYDTDTE in SAS which supports min date
    # 01JAN1582
    # pylint: disable=import-outside-toplevel
    from datetime import datetime as dt, date

    def parse_date(param):
        try:

            if dt.strptime(param, "%Y%m%d").date() >= date(1582, 1, 1):
                return dt.strptime(param, "%Y%m%d").date()

            if param[:2] == "00":
                value = dt.strptime(param, "%Y%m%d").date()

                # by empirical tests we found that leading '00's up to (current year + 1)
                # are recognized as (2000 + value.year), if the value exceeds (current year + 1) -
                # it is recognized as (1900 + value.year)
                # for example: '000190120' as of year 2024 will be parsed as '2019-01-20' and '00730120'
                # will be parsed as '1973-01-20'
                if value.year + 2000 <= date.today().year + 1:
                    return dt(value.year + 2000, value.month, value.day).date()

                return dt(value.year + 1900, value.month, value.day).date()

            raise ValueError("Minimum value exception")
        except ValueError:

            if dt.strptime(param, "%d%m%Y").date() >= date(1582, 1, 1):
                return dt.strptime(param, "%d%m%Y").date()

            raise 

    param1 = "" if not param1 else param1
    param2 = "" if not param2 else param2

    try:
        result = parse_date(param1)
    except ValueError:
        try:
            result = parse_date(param2)
        except ValueError as e:
            print(f"An exception occurred: {str(e)}")
            result = None

    if result and 0 < result.year < 10:
        result = dt(result.year + 2000, result.month, result.day).date()

    return result

# case when &datumtijd. = . then &default. else datepart(&datumtijd.) - &correctie. end
# Bepalen datum op basis van datumtijd veld met opties
# voor vullen wanneer lege waarde en correctie in dagen
# Determine date based on datetime field with options
# for filling when empty value and correction in days
@udf(returnType = DateType())
def CZ_SAS_BR_T_DATUMTIJD_DATEPART(datumtijd, default, correctie):
    # pylint: disable=import-outside-toplevel
    from datetime import datetime, date, timedelta

    def datepart(date_time):

        if isinstance(date_time, date):
            return date_time

        if isinstance(date_time, datetime):
            return date_time.date()

        try:
            result = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                result = datetime.strptime(date_time, "%Y-%m-%dT%H:%M:%S")
            except ValueError as e:
                raise ValueError(f"Input date '{date_time}' is not in allowed formats") from e

        return result

    if default:
        default_obj = datepart(default)
    else:
        default_obj = None

    result = (default_obj if datumtijd is None else datepart(datumtijd) - timedelta(days = correctie))

    return result

@udf(returnType = TimestampType())
def CZ_SAS_BR_T_DATUM_TIJD(datumveld: str, tijdveld: str):
    # pylint: disable=import-outside-toplevel
    from datetime import datetime, timedelta

    try:

        if (
            (datumveld is None)
            or (tijdveld is None)
            or (tijdveld == "  0000")
            or (
              len(tijdveld) != 6
              and tijdveld != "00002"
            )
        ):
            return None

        # Parse the date (datumveld) in 'YYYYMMDD' format
        date_part = datetime.strptime(datumveld, "%Y%m%d")

        if tijdveld in ("00.00.", "0.0000"):
            tijdveld = "000000"

        # Extract time components from tijdveld
        hours = int(tijdveld[0:2])
        minutes = int(tijdveld[2:4])
        seconds = int(tijdveld[4:6])
        # Create a datetime object from the date and time components
        datetime_part = date_part + timedelta(hours = hours, minutes = minutes, seconds = seconds)

        return datetime_part
    except Exception:
        return None

@udf(returnType = StringType())
def CZ_SAS_BR_T_DBC_PERIODE(verrichting_ingang_dtm, verrichting_eind_dtm) -> str:
    result = f"{verrichting_ingang_dtm.year}-{verrichting_eind_dtm.year}"

    return result

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_DDD(atl_ddd_per_pdc, ied):

    if atl_ddd_per_pdc is None or ied is None or ied == 0:
        return None

    return atl_ddd_per_pdc / ied

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_DD_HEROPEND_IDC(status, activiteit) -> int:

    if status == "120" or "proces heropend".upper() in (activiteit or "").upper():
        return 1

    return 0

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_DD_TOEKOMST_IDC(status, activiteit) -> int:

    if status == "600" or "proces in de toekomst" in activiteit.upper():
        result = 1 # Toekomst
    else:
        result = 0 # Geen toekomst

    return result

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_DD_XTRA_INFO_IDC(status, activiteit) -> int:

    if status == "400" or ("EXTRA INFORMATIE OPGEVRAAGD" in upper(activiteit)):
        return 1

    return 0

@udf(returnType = StringType())
def CZ_SAS_BR_T_DECLARATIECODE_OVP(
        externe_code,
        verrichting_soort_code,
        verrichting_stelsel_code,
        verrichting_subsoort_code,
) -> str:

    if externe_code != "":
        result = externe_code
    elif verrichting_soort_code == "51" and verrichting_stelsel_code == "1":
        # substr in python is achieved with slicing [start: end], and is
        # 0-indexed
        result = verrichting_subsoort_code[6:12]
    else:
        result = ""

    return result

@udf(returnType = StringType())
def CZ_SAS_BR_T_DECLARATIESOORT(externe_code) -> str:

    if externe_code is None or len(externe_code) < 2:
        return ""

    return externe_code[:2]

@udf(returnType = StringType())
def CZ_SAS_BR_T_DV_PERSONEELSNUMMER(dv_usernummer) -> str:

    if (dv_usernummer is not None and len(re.sub("[^1234567890]", "", dv_usernummer)) == 6):
        return re.sub("[^1234567890]", "", dv_usernummer)

    return "-1"

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_EIGEN_RISICO_ZVW(kostensoort_code) -> int:

    if kostensoort_code in ("697700", "697710"):
        return 1

    return 0

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_ELFPROEF(param: str):
    try:
        param_str = str(param)

        if param_str.isdigit() and len(param_str) == 9:
            total = 0
            total = (
                int(param_str[0]) * 9
                + int(param_str[1]) * 8
                + int(param_str[2]) * 7
                + int(param_str[3]) * 6
                + int(param_str[4]) * 5
                + int(param_str[5]) * 4
                + int(param_str[6]) * 3
                + int(param_str[7]) * 2
                - int(param_str[8])
            )

            if total % 11 == 0:
                result = 1
            else:
                result = 0

            return result

        return 0
    except Exception:
        return 0

@udf(returnType = StringType())
def CZ_SAS_BR_T_EMAIL_AKTIVITEIT(aktivitet) -> str:

    if aktivitet is None:
        return None

    index_aan = aktivitet.find(" aan ")
    index_naar = aktivitet.find(" naar ")

    if index_aan > 0 and index_naar == - 1:
        return aktivitet[:index_aan]

    if index_naar > 0 and index_aan == - 1:
        return aktivitet[:index_naar]

    if index_aan > 0 and index_naar > 0:
        ind = index_aan if index_aan < index_naar else index_naar

        return aktivitet[:ind]

    return aktivitet

@udf(returnType = StringType())
def CZ_SAS_BR_T_EMAIL_STATUS(statusoms):

    if statusoms is None:
        return None

    statusoms_lower = statusoms.lower()

    if "emailbericht" in statusoms_lower:
        start_index = statusoms_lower.find("emailbericht")

        if start_index == 0:
            naar_index = statusoms_lower.find(" naar")

            if naar_index != - 1:
                result = statusoms[start_index + len("emailbericht") + 1:naar_index]
            else:
                result = statusoms[start_index + len("emailbericht") + 1:]
        else:
            result = (statusoms[:start_index] + statusoms[start_index + len("emailbericht") + 1:])
    else:
        result = statusoms

    return result.replace(
        " ",
        "#"
    )

# =======================
# == Original SQL code ==
# =======================
@udf(returnType = StringType())
def CZ_SAS_BR_T_FICTIEF_LEEGMAKEN(id_num: float, veld: str) -> str:

    if id_num is None or id_num < 0:
        return None

    return veld

@udf(returnType = StringType())
def CZ_SAS_BR_T_FOUT_ONZEKERHEID_STATUS(schade_notaregel_status, schade_correctie_idc) -> str:

    if schade_notaregel_status in ("GOEDGEKEURD", "VERVALLEN"):
        return "Correct"

    if (schade_notaregel_status in ("AKKOORD", "NIET AKKOORD", "NIET BEOORDEELD") and schade_correctie_idc == 1):
        return "Gecorrigeerd"

    if schade_notaregel_status == "AKKOORD" and schade_correctie_idc == 0:
        return "Fout"

    if (schade_notaregel_status in ("NIET AKKOORD", "NIET BEOORDEELD") and schade_correctie_idc == 0):
        return "Onzeker"

    return "Onbepaald"

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_GECONTRACTEERD(zorgverlener_ovk_soort_nmr) -> int:
    # define the values we want to check for
    check_values = {"900", "901", "000", "950", "960", " "}
    # extract the substring to check
    substr_value = (None if zorgverlener_ovk_soort_nmr is None else zorgverlener_ovk_soort_nmr[5:8])
    # return 0 if the substring is in the list of check values, else return 1
    result = 0 if substr_value in check_values else 1

    return result

@udf(returnType = StringType())
def CZ_SAS_BR_T_GESLACHT(geslacht):
    geslacht = geslacht.lower() if geslacht else ""

    try:

        if geslacht == "m":
            return "1"

        if geslacht in ("v", "f"):
            return "2"

        return "0"
    except Exception as e:
        print(f"An exception occurred: {str(e)}")

        return geslacht

@udf(returnType = StringType())
def CZ_SAS_BR_T_GPH_CODE(gph_nigella_code, gph_conversie_code, gph_zorgverlener_code) -> str:

    if gph_nigella_code is not None:
        return gph_nigella_code

    if gph_conversie_code is not None:
        return gph_conversie_code

    if gph_zorgverlener_code is not None:
        return gph_zorgverlener_code

    return ""

@udf(returnType = StringType())
def CZ_SAS_BR_T_HOOFDLETTERS(an_kolom):
    an_kolom = an_kolom.upper() if an_kolom else None

    return an_kolom

@udf(returnType = StringType())
def CZ_SAS_BR_T_HUISNUMMER(huisnummer: str):

    # check the input parameter
    if huisnummer is None:
        return None

    # check the position of the first non-digit to return all digits prior to
    # it
    for i, char in enumerate(huisnummer):
        if not char.isdigit():
            return huisnummer[:i]

    # otherwise return the input parameter
    return huisnummer

@udf(returnType = StringType())
def CZ_SAS_BR_T_HUISNUMMER_TOEVOEGING(huisnummer, huisnummertoevoeging) -> str:
    first_non_digit_index = re.search(r"\D", huisnummer) # find the first non-digit character

    if first_non_digit_index:
        result = huisnummer[first_non_digit_index.start():] # substring from the first non-digit character to the end
    else:
        result = huisnummertoevoeging

    return result

@udf(returnType = StringType())
def CZ_SAS_BR_T_INDICATIE_SOORT(verrichting_cluster_code, paramedische_hulp_idc_code) -> str:
    chronische_codes = {
"001", "002", "008", "010", "011", "012", "013", "014", "015", "016", "017", }
    niet_chronische_codes = {"003", "004", "005", "006", "007", "009"}
    geldige_codes = {"1", "2", "7"}

    if verrichting_cluster_code in geldige_codes:

        if paramedische_hulp_idc_code in chronische_codes:
            return "1.Chronisch"

        if paramedische_hulp_idc_code in niet_chronische_codes:
            return "2.Niet Chronisch"

        if paramedische_hulp_idc_code != "":
            return "3.Foutief"

        return "4.Onbekend"

    return "4.Onbekend"

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_INDICATOR(indicator, geldige_waarde):

    if indicator == geldige_waarde:
        return 1.0

    return 0.0

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_INGEDIEND_DOOR_VERZEKERDE_I(declarant_type_code: str) -> int:
    return 1 if declarant_type_code == "0" else 0

@udf(returnType = StringType())
def CZ_SAS_BR_T_INKOOPKANAAL_TYPE(verrichting_inkoopkanaal_code) -> str:

    if verrichting_inkoopkanaal_code in ("1", "3", "6"):
        result = "Specialite"
    elif verrichting_inkoopkanaal_code in ("2", "4"):
        result = "Generiek"
    elif verrichting_inkoopkanaal_code in ("0", "5"):
        result = "Overig"
    else:
        result = ""

    return result

# VERRICHTING_INKOOPKANAAL_CODE    BR_T_INKOOPKANAAL_TYPE_NUM    Technical
# case when &VERRICHTING_INKOOPKANAAL_CODE in (1,3,6) then 'Specialite'
# when &VERRICHTING_INKOOPKANAAL_CODE in(2,4) then 'Generiek'
# when &VERRICHTING_INKOOPKANAAL_CODE in (0,5) then 'Overig'
# else '' end
# Bepaling VERRICHTING_INKOOPKANAAL_TYPE met numerieke bronkolom
@udf(returnType = StringType())
def CZ_SAS_BR_T_INKOOPKANAAL_TYPE_NUM(verrichting_inkoopkanaal_code):

    if verrichting_inkoopkanaal_code is None:
        return ""

    if int(verrichting_inkoopkanaal_code) in (1, 3, 6):
        return "Specialite"

    if int(verrichting_inkoopkanaal_code) in (2, 4):
        return "Generiek"

    if int(verrichting_inkoopkanaal_code) in (0, 5):
        return "Overig"

    return ""

@udf(returnType = StringType())
def CZ_SAS_BR_T_INSTITUTIONELE_ZVLSOORT(column):
    valid_values = {"30", "35", "40", "42", "43", "45", "46", "47", "48", "65", "72"}

    if column in valid_values:
        return column

    return None # or some default value, depending on the requirement

@udf(returnType = StringType())
def CZ_SAS_BR_T_KETENZORG_SOORT(verrichting_subsoort_code) -> str:

    if verrichting_subsoort_code in ("1.01.000000011625", "1.01.000000040001"):
        result = "DM2"
    elif verrichting_subsoort_code in (
"1.01.000000011626", "1.01.000000011627", "1.01.000000040021", ):
        result = "COPD"
    elif verrichting_subsoort_code in (
"1.01.000000031292", "1.01.000000031293", "1.01.000000040011", ):
        result = "VRM"
    else:
        result = "ONBK"

    return result

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_KEY_FOUT_DBC(
        periode_grootboek,
        agb_code_uitvoerder,
        kostensoort_code,
        verrichting_soort_code,
        verrichting_stelsel_code,
        externe_code,
):

    if periode_grootboek[:4] <= "2011" and agb_code_uitvoerder[5:11] == "280501":
        return 0

    if (
        kostensoort_code in ("617000", "618100")
        and verrichting_soort_code != "51"
        and verrichting_stelsel_code != "1"
    ):
        return 1

    if kostensoort_code in ("613000", "613100", "614000") and externe_code[:2] != "14":
        return 1

    if kostensoort_code in ("615000", "615100", "616000") and externe_code[:2] != "15":
        return 1

    if kostensoort_code == "619000" and externe_code[:2] in ("16", "17"):
        return 1

    if kostensoort_code == "619000" and verrichting_soort_code not in ("51", "54"):
        return 1

    return 0

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_KEY_OVERFINANCIERING(
        externe_code,
        verrichting_soort_code,
        verrichting_stelsel_code,
        verrichting_subsoort_code,
):

    if (
        externe_code.startswith("  ")
        and (
          verrichting_soort_code == "51"
          and verrichting_stelsel_code == "2"
          and verrichting_subsoort_code in ("000000009990", "000000009997")
        )
    ):
        return 1

    return 0

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_KM(atl_kmr_vei):

    try:
        result = int(atl_kmr_vei)
    except (ValueError, TypeError):
        result = None

    return result

@udf(returnType = StringType())
def CZ_SAS_BR_T_KOSTENSOORT_OMS(desc, rbinh015):
    return desc if desc is not None else rbinh015

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_KREDIETTERMIJNCATEGORIE(krediettermijn) -> int:

    if 0 <= krediettermijn <= 30:
        return 1

    if 31 <= krediettermijn <= 60:
        return 2

    if 61 <= krediettermijn <= 90:
        return 3

    if 91 <= krediettermijn <= 120:
        return 4

    if 121 <= krediettermijn <= 150:
        return 5

    if 151 <= krediettermijn <= 180:
        return 6

    if krediettermijn >= 181:
        return 7

    return 0

@udf(returnType = StringType())
def CZ_SAS_BR_T_LABEL(kde_jet) -> str:
    mapping = {"0010" : "004", "0020" : "005", "0021" : "005"}

    return mapping.get(kde_jet, "001")

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_MASKEREN_TIJD_INTERVAL() -> int:
    return 7

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_MA_AFGEHANDELD_IDC(code: str) -> int:
    try:

        if code is None:
            return 0

        if (
            code
            in ("Goedgekeurd", "Goedgekeurd door Vecozo", "Gedeeltelijk goedgekeurd", "Afgekeurd",
               "Afgekeurd door Vecozo", "Vervallen", "Verwijderd", "Beëindigd door zorgverzekeraar",)
        ):
            return 1

        return 0
    except Exception as e:
        print(f"An exception occurred: {str(e)}")

        return 0

@udf(returnType = StringType())
def CZ_SAS_BR_T_MA_BEPAAL_AFDELING(status):

    if status.startswith("Dossier voorgelegd"):
        return "45700"

    if status.startswith("Nieuw proces reeds eerder toegewezen - Medisch Advies Groep"):
        return "45700"

    return "62003a"

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_MA_BINNEN_DE_NORM(param):

    # CASE WHEN &DOORLOOPTIJD_WERKDAGEN_BR <= 10 THEN 1 ELSE 0 END
    if param <= 10:
        return 1

    return 0

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_MA_DOORLOOPTIJD_DAGEN() -> int:
    return 0

# Business Rule: cz_sas_br_t_ma_doorlooptijd_werkdagen
# SAS Code: CASE WHEN &AFGEHANDELD_IDC = 0 THEN intck('weekday', DATEPART(&POST_DTD), &PEIL_DTM)
# ELSE intck('weekday', DATEPART(&POST_DTD), &AFHANDEL_DTM) END
@udf(returnType = IntegerType())
def CZ_SAS_BR_T_MA_DOORLOOPTIJD_WERKDAGEN(post_dtd, peil_dtm, afhandel_dtm, afgehandeld_idc):

    def count_weekdays(start_date, end_date):

        if start_date is None or end_date is None or start_date > end_date:
            return None

        total_days = (end_date - start_date).days + 1
        weekdays = 0

        for day in range(total_days):
            current_day = (start_date + datetime.timedelta(days = day)).weekday()

            # Monday = 0, Sunday = 6, so exclude Saturday and Sunday
            if current_day < 5:
                weekdays += 1

        return weekdays

    if afgehandeld_idc == 0:
        end_date = peil_dtm
    else:
        end_date = afhandel_dtm

    post_dtd_date = (to_date(col(post_dtd)).cast("date") if isinstance(post_dtd, str) else post_dtd)
    end_date = (to_date(col(end_date)).cast("date") if isinstance(end_date, str) else end_date)

    return count_weekdays(post_dtd_date, end_date)

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_MA_EXTRA_INFO_IDC(activiteit_code) -> int:
    return 1 if activiteit_code == "Extra informatie opgevraagd" else 0

@udf(returnType = StringType())
def CZ_SAS_BR_T_MA_GROEPEER_STATUS(status):
    patterns = [(r"Dossier toegewezen aan", lambda m: m.group(0)),
                (r"Collectiviteitsnummer gewijzigd naar", lambda m: m.group(0)),
                (r"Gedelegeerd aan", lambda m: m.group(0)),
                (r"Naar status Proces in de toekomst", lambda m: m.group(0)),
                (r"Relatienummer gewijzigd naar", "Relatienummer gewijzigd naar"),
                (r"Relatienummer \d+? vervallen", "Relatienummer vervallen"),
                (r"Subproces gewijzigd naar", "Subproces gewijzigd naar"),
                (r"Ontkoppeld van dossiernummer", "Ontkoppeld van dossiernummer"),
                (r"Groep gewijzigd naar", "Groep gewijzigd naar"),
                (r"Naar status Proces afgehandeld", "Naar status Proces afgehandeld"),
                (r"Subproces \D+? vervallen", "Subproces vervallen"),
                (r"Extra informatie opgevraagd", "Extra informatie opgevraagd"),
                (r"Aanvrager .* vervallen", "Aanvrager vervallen"),
                (r"Aanvrager gewijzigd", "Aanvrager gewijzigd"),
                (r"Proces in de toekomst", "Proces in de toekomst"),
                (r"Uitvoerder .* vervallen", "Uitvoerder vervallen"),
                (r"Uitvoerder gewijzigd naar", "Uitvoerder gewijzigd"),
                (r"Toegewezen aan", "Toegewezen aan"),]

    for pattern, replacement in patterns:
        match = re.match(pattern, status)

        if match:

            if callable(replacement):
                return replacement(match).strip()

            return replacement

    return status.strip()

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_MA_HEROPEND_IDC(activiteit_code):

    if activiteit_code == "Proces heropend":
        return 1

    return 0

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_MA_INTOEKOMST_IDC(activiteit_code) -> int:

    if activiteit_code == "Proces in de toekomst":
        result = 1
    else:
        result = 0

    return result

@udf(returnType = StringType())
def CZ_SAS_BR_T_MA_KANAAL(vecozotype, n_cmindicatiedocument) -> str:

    if vecozotype is not None:
        return vecozotype

    if n_cmindicatiedocument is not None:
        return n_cmindicatiedocument[:12]

    return ""

@udf(returnType = StringType())
def CZ_SAS_BR_T_MA_LAND(landcode):
    return "NL" if landcode is None else landcode

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_MA_PZP_IDC(politiecollectiviteit) -> int:
    return 1 if politiecollectiviteit == "JA" else 0

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_MA_TOEGEWEZEN_IDC(activiteit_code) -> int:

    if activiteit_code is not None and "toegewezen" in activiteit_code.lower():
        return 1

    return 0

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_MA_VERWIJDEREN_IDC(verwijderen) -> int:

    if verwijderen == "JA":
        result = 1
    else:
        result = 0

    return result

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_MA_VOORGELEGD_IDC(activiteit_code) -> int:

    if activiteit_code and "dossier voorgelegd" in activiteit_code.lower():
        return 1

    return 0

@udf(returnType = StringType())
def CZ_SAS_BR_T_MA_ZVL_CONCAT(zvl_soort, zvl_agb_code) -> str:

    if zvl_agb_code == "":
        return ""

    return f"{zvl_soort}.{zvl_agb_code}"

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_MIN_VERWIJDER_DTD(edwh_verwijder_dtd):

    if edwh_verwijder_dtd is None:
        return 0.0

    return float(min(edwh_verwijder_dtd))

@udf(returnType = StringType())
def CZ_SAS_BR_T_NAAM() -> str:
    return ""

def CZ_SAS_BR_T_NATUURLIJK_PERS_LEEFTIJDGenerator():
    """Determine the age of a natural person based on date of birth and reference date"""
    # an init file required for the code to get compiled with date module imported
    # this makes code.py run
    # pylint: disable=W0611
    from datetime import date, datetime

    # RULE_ID   : 91
    # RULE_PARMS: "geboortedatum, referentie_dtm"
    # PARMS_ENG : birth_date, reference_dtm,
    # + added syspeildatum for default date
    # since it is accessible in SAS, but not in Dbx
    # RULE_NAME : BR_T_NATUURLIJK_PERS_LEEFTIJD
    # RULE_ENG  : NATURAL PERSON AGE
    # RULE_CODE : "FLOOR( (INTCK('MONTH', &geboortedatum.,COALESCE(&referentie_dtm.,&syspeildatum.))
    #             - (DAY(COALESCE(&referentie_dtm.,&syspeildatum.)) < MIN ( DAY(&geboortedatum.),
    #             DAY (INTNX ('MONTH',COALESCE(&referentie_dtm.,&syspeildatum.), 1) - 1) ) ) ) /12 )"
    # RULE_TEXT : Bepaal leeftijd natuurlijk persoon op basis van geboortedatum en peildatum
    # text_eng  : Determine the age of a natural person based on date of birth and reference date
    # RULE_NOTE : Op basis van de geboortedatum en peildatum wordt de werkelijke leeftijd in jaren berekend
    # note_eng  : The actual age in years is calculated based on the date of birth and reference date
    # STATUS    : A
    @udf(returnType = DoubleType())
    def func(birth_date: date, reference_dtm: date, syspeildatum: date) -> float:
        # pylint: disable=import-outside-toplevel
        from dateutil.relativedelta import relativedelta

        if birth_date is None:
            return None

        coalesce_expr = syspeildatum if reference_dtm is None else reference_dtm

        # There have been a thoroughly translated from SAS to Python function.
        # But I have changed it to a simple and more efficient Python one-liner.
        return float(relativedelta(coalesce_expr, birth_date).years)

    return func

CZ_SAS_BR_T_NATUURLIJK_PERS_LEEFTIJD = CZ_SAS_BR_T_NATUURLIJK_PERS_LEEFTIJDGenerator()

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_NORM_NAAR_UREN(eenheid, waarde):

    if eenheid == "D":
        result = waarde * 24
    else:
        result = None

    return result

# =======================
# == Original SAS code ==
# =======================
@udf(returnType = StringType())
def CZ_SAS_BR_T_NUMERICFIELD_TOCHAR9(value):

    if value is None:
        return None

    # Convert numeric value to string with width of 11 characters
    # In addition applied convertation to integer to avoid a string with ".0"
    # representation
    str_value = str(int(value))

    # Check if the length of the string is greater than 9
    if len(str_value) > 9:
        # If so, truncate the string to the first 9 characters
        return str_value[:9]

    # Otherwise, return the original string
    return str_value

# Name: cz_sas_br_t_numeriek
# SAS Code: case when &CHAR_WAARDE ne '' then input(&CHAR_WAARDE,&FORMAT)
# else &DEFAULT end
@udf(returnType = DoubleType())
def CZ_SAS_BR_T_NUMERIEK(char_waarde, default) -> float:

    if char_waarde != "":
        try:
            result = float(char_waarde)
        except ValueError:
            result = default # if conversion fails, return default value
    else:
        result = default

    return result

# =======================
# == Original SQL code ==
# =======================
@udf(returnType = StringType())
def CZ_SAS_BR_T_NVT_LEEGMAKEN(code: str) -> str:

    if code == "!@":
        return None

    return code

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_ONTSLEUTELEN(indicatie_veld: str) -> float:
    try:

        if indicatie_veld is None:
            return 0.0

        return 1.0 if indicatie_veld == "VG" else 0.0
    except Exception as e:
        print(f"An exception occurred: {str(e)}")

        return 0.0

# Business Rule: cz_sas_br_t_postcode
# SAS Code:
# ifc (translate (upcase (compress (&postcode.)),
#                 repeat ('#', 9) || repeat ('$', 25),
#                 collate (48, 57) || collate (65, 90)) = '####$$',
#     &postcode., '')
@udf(returnType = StringType())
def CZ_SAS_BR_T_POSTCODE(postcode):

    if postcode is None:
        return ""

    postcode_compressed = re.sub("[\\s+-]", "", postcode.upper())
    translated = postcode_compressed.translate(
        str.maketrans(
          "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ",
          "#########$$$$$$$$$$$$$$$$$$$$$"
        )
    )

    if (
        translated
        == "####$$"
    ):
        return postcode

    return ""

@udf(DoubleType())
def CZ_SAS_BR_T_PREMIEGRONDSLAG_BDG(fabrieksprijs_bdg, commerciele_toeslag_bdg, leeftijd_bdg, betaaltermijn_bdg):
    """
    Calculate the sum of given parameters, handling None values as zeros.

    :param fabrieksprijs_bdg: The factory price
    :param commerciele_toeslag_bdg: The commercial surcharge
    :param leeftijd_bdg: The age
    :param betaaltermijn_bdg: The payment term
    :return: The sum of the input parameters
    """

    def convert_to_float(value):

        if value is None:
            return 0.0

        if isinstance(value, float):
            return value

        raise ValueError(f"Invalid value type: {value}.")

    fabrieksprijs_bdg = convert_to_float(fabrieksprijs_bdg)
    commerciele_toeslag_bdg = convert_to_float(commerciele_toeslag_bdg)
    leeftijd_bdg = convert_to_float(leeftijd_bdg)
    betaaltermijn_bdg = convert_to_float(betaaltermijn_bdg)

    return (fabrieksprijs_bdg + commerciele_toeslag_bdg + leeftijd_bdg + betaaltermijn_bdg)

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_PREMIEPLICHTIG(netto_premie_bdg: float) -> float:
    netto_premie_bdg = netto_premie_bdg or float(0.0)

    if netto_premie_bdg > 0.005:
        return float(1.0)

    return float(0.0)

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_PREMIEVRIJE_VERZ_IDC(idc_cpv_pij_vzd: str, idc_aps_pij_vzd: str) -> float:
    try:
        return 1.0 if idc_cpv_pij_vzd == "J" or idc_aps_pij_vzd == "J" else 0.0
    except Exception as e:
        print(f"An exception occurred: {str(e)}")

        return 0.0

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_PROVISIE_REGELING_BDG(bdg_cpv: float, bdg_aps: float) -> float:
    try:
        return bdg_cpv if bdg_cpv > 0.0 else (bdg_aps if bdg_aps > 0.0 else 0.0)
    except Exception as e:
        print(f"An exception occurred: {str(e)}")

        return 0.0

@udf(returnType = StringType())
def CZ_SAS_BR_T_PROVISIE_REGELING_CODE(
        provisie_rgl_type_code: float,
        provisie_regeling_bdg: float,
        provisie_regeling_pct: float,
        provisie_premievrije_verz_idc: float,
) -> str:
    try:
        return (
            provisie_rgl_type_code.strip()
            + format(float(provisie_regeling_bdg), "8.2f").strip().replace(".", ",")
            + format(float(provisie_regeling_pct), "8.2f").strip().replace(".", ",")
            + format(int(provisie_premievrije_verz_idc), "1d").strip().replace(".", ",")
        )
    except Exception as e:
        print(f"An exception occurred: {str(e)}")

        return ""

@udf(returnType = StringType())
def CZ_SAS_BR_T_PROVISIE_REGELING_OMS(
        provisie_rgl_type_code: str,
        provisie_regeling_bdg: float,
        provisie_regeling_pct: float,
        provisie_premievrije_verz_idc: float,
) -> str:
    try:

        if (provisie_rgl_type_code == "CP" and provisie_premievrije_verz_idc == 1 and provisie_regeling_bdg > 0):
            return f"{np.round(12 * provisie_regeling_bdg * 2) / 2:,.2f} voor alle verzekerden".replace(".", ",")

        if (provisie_rgl_type_code == "CP" and provisie_premievrije_verz_idc == 0 and provisie_regeling_bdg > 0):
            return f"{np.round(12 * provisie_regeling_bdg * 2) / 2:,.2f} voor premiebetalende verzekerden".replace(
                ".",
                ","
            )

        if (provisie_rgl_type_code == "AP" and provisie_premievrije_verz_idc == 1 and provisie_regeling_bdg > 0):
            return f"{provisie_regeling_bdg:,.2f} voor alle verzekerden".replace(".", ",")

        if (provisie_rgl_type_code == "AP" and provisie_premievrije_verz_idc == 0 and provisie_regeling_bdg > 0):
            return f"{provisie_regeling_bdg:,.2f} voor premiebetalende verzekerden".replace(".", ",")

        if provisie_regeling_pct > 0 and (np.round(provisie_regeling_pct, 1) != provisie_regeling_pct):
            return f"{provisie_regeling_pct:.2f}% van de premie".replace(".", ",")

        if provisie_regeling_pct > 0:
            return f"{provisie_regeling_pct:.1f}% van de premie".replace(".", ",")

        if provisie_regeling_bdg == 0 and provisie_regeling_pct == 0:
            return "0,0% van de premie"

        return ""
    except Exception as e:
        print(f"An exception occurred: {str(e)}")

        return ""

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_PROVISIE_REGELING_PCT(pct_cpv: float, pct_aps: float) -> float:
    try:
        return pct_cpv if pct_cpv > 0.0 else (pct_aps if pct_aps > 0.0 else 0.0)
    except Exception as e:
        print(f"An exception occurred: {str(e)}")

        return 0.0

@udf(returnType = StringType())
def CZ_SAS_BR_T_PROVISIE_RGL_TYPE_CODE(pct_cpv: float, bgd_cpv: float, pct_aps: float, bgd_aps: float) -> str:
    try:

        if pct_cpv > 0 or bgd_cpv > 0:
            result = "CP"
        elif pct_aps > 0 or bgd_aps > 0:
            result = "AP"
        else:
            result = "CP"

        return result
    except Exception as e:
        print(f"An exception occurred: {str(e)}")

        return ""

# code: compress(strip(put(&variabelenaam, &lengte..)),'.')
# params: VARIABELENAAM, LENGTE
# text: Haalt punt weg bij omzetten leeg numeriek veld naar character
# text_en: Removes point when converting empty numeric field to character
@udf(returnType = StringType())
def CZ_SAS_BR_T_PUNT_LEEGMAKEN(value: float, in_format: str) -> str:

    # Ensure the input value is not None
    if value is None:
        return None

    # Extract the numeric length from the format string (e.g., z9 or z2)
    if in_format.startswith("z") and in_format[1:].isdigit():
        # Extract the desired length from the format (e.g., 9 for z9)
        length = int(in_format[1:])
        # Convert the value to an integer and strip leading/trailing spaces
        result = str(int(float(value)))
        result = result.strip().replace(".", "")

        # If the result is longer than the specified length, truncate it
        if len(result) > length:
            result = result[:length]

        # Pad the result with leading zeros to match the specified length
        return result.zfill(length)

    # Raise an error for unsupported formats
    raise ValueError(f"Unsupported format: {in_format}")

# RULE_ID   : 158
# RULE_PARMS: OVEREENKOMSTNUMMER_MOEDER
# RULE_NAME : BR_T_PZP_COLLECTIVITEIT_IDC
# RULE_CODE : CASE WHEN &OVEREENKOMSTNUMMER_MOEDER = '002153564' THEN 1 ELSE 0 END
# RULE_TEXT : Bepalen of een moedercollectiviteit een PZP collectiviteit is
# TEXT_EN   : Determine whether a parent collectivity is a PZP collectivity
# STATUS    : A
@udf(returnType = BooleanType())
def CZ_SAS_BR_T_PZP_COLLECTIVITEIT_IDC(overeenkomstnummer_moeder: str) -> bool:

    if overeenkomstnummer_moeder == "002153564":
        return True

    return False

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_RENDEMENT_LABELORG(
        commerciele_toeslag_periode_bdg,
        buitenland_periode_bdg,
        pakket_periode_bdg,
        collectiviteit_periode_bdg,
        commerc_toeslag_er_periode_bdg,
        continuteitsprov_periode_bdg,
        afsluitprovisie_bdg,
) -> float:
    """cz_sas_br_t_rendement_labelorg"""
    # pylint: disable=import-outside-toplevel
    from math import fsum
    components = [commerciele_toeslag_periode_bdg, buitenland_periode_bdg, pakket_periode_bdg,
                  collectiviteit_periode_bdg, commerc_toeslag_er_periode_bdg,
                  continuteitsprov_periode_bdg, afsluitprovisie_bdg,]
    result = float(fsum(c if c is not None else 0 for c in components))

    return result

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_RENDEMENT_LABELORG_MAAND(
        commerciele_toeslag_bdg,
        buitenland_maand_bdg,
        pakket_maand_bdg,
        collectiviteit_bdg,
        commerc_toeslag_er_maand_bdg,
        continuiteitsprovisie_maand_bdg,
        afsluitprovisie_maand_bdg,
) -> float:
    """UDF cz_sas_br_t_rendement_labelorg_maand"""
    # pylint: disable=import-outside-toplevel
    from math import fsum
    components = [commerciele_toeslag_bdg, buitenland_maand_bdg, pakket_maand_bdg, collectiviteit_bdg,
                  commerc_toeslag_er_maand_bdg, continuiteitsprovisie_maand_bdg,
                  afsluitprovisie_maand_bdg,]
    result = float(fsum(c if c is not None else 0 for c in components))

    return result

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_RESTITUTIE_BEDRAG_BRK(restitutie_bdg, restitutie_tegenboeking_bdg) -> float:
    result = (
        (float(restitutie_bdg) if restitutie_bdg is not None else 0.0)
        + (float(restitutie_tegenboeking_bdg) if restitutie_tegenboeking_bdg is not None else 0.0)
    )

    return result

@udf(returnType = StringType())
def CZ_SAS_BR_T_SELECTIE_DATUM():
    return ""

@udf(returnType = StringType())
def CZ_SAS_BR_T_SOORT_HONORARIUM_KOSTEN(uitvoerder_soort_code: str) -> str:

    if uitvoerder_soort_code in ("03", "11"):
        result = "Honorarium"
    else:
        result = "Kosten"

    return result

@udf(returnType = StringType())
def CZ_SAS_BR_T_SPECIALISME(zorgverlener_subsoort_naam_int) -> str:

    if zorgverlener_subsoort_naam_int == "":
        result = "Niet specialismespecifiek"
    else:
        result = zorgverlener_subsoort_naam_int

    return result

@udf(returnType = StringType())
def CZ_SAS_BR_T_SPECIALISME_DIAGNOSE(zorgverlener_subsoort_code, dot_diagnose_code):

    if zorgverlener_subsoort_code and dot_diagnose_code:
        return f"{zorgverlener_subsoort_code}-{dot_diagnose_code}"

    return ""

@udf(returnType = DateType())
def CZ_SAS_BR_T_START_KREDIETTERMIJN(dc_nota_categorie, ontstaan_dtm):

    if dc_nota_categorie in [
"Zorgverzekering", "Aanvullende verz.", "Ziektekosten", "Ziekenfonds", ]:
        if ontstaan_dtm is not None:
            year, month = ontstaan_dtm.year, ontstaan_dtm.month
            new_month = month % 12 + 1
            new_year = year + (month + 1) // 13
            last_day_of_new_month = calendar.monthrange(new_year, new_month)[1]
            result_date = datetime(new_year, new_month, last_day_of_new_month)
        else:
            result_date = None
    else:
        result_date = ontstaan_dtm

    return result_date

@udf(returnType = StringType())
def CZ_SAS_BR_T_STEDELIJKHEID_OMS(code_sted):

    if code_sted == 1:
        return "Zeer sterk stedelijk"

    if code_sted == 2:
        return "Sterk stedelijk"

    if code_sted == 3:
        return "Matig stedelijk"

    if code_sted == 4:
        return "Weinig stedelijk"

    if code_sted == 5:
        return "Niet stedelijk"

    return "Niet te bepalen"

# paramkolom       Name of the column to process
# lengte           The length of the output string (number of characters, including leading zeros)
# The function pads the input value with leading zeros to match the
# specified total length
@udf(StringType())
def CZ_SAS_BR_T_STRING_VOORLOOPNULLEN(paramkolom, lengte: int) -> str:

    if paramkolom is None:
        paramkolom = ""

    stripped_paramkolom = str(paramkolom).strip()
    reversed_paramkolom = stripped_paramkolom[::- 1]
    padded_paramkolom = (reversed_paramkolom + "0000000000")[:lengte]
    result = padded_paramkolom[::- 1]

    return result.zfill(lengte)

@udf(returnType = StringType())
def CZ_SAS_BR_T_SUBSTR_MUTATIETOTAAL(veld, beginstring, eindstring) -> str:

    if beginstring in veld:
        start_pos = veld.find(beginstring)
        end_pos = veld.find(eindstring)
        begin_len = len(beginstring)

        if end_pos == - 1 or (end_pos - start_pos == begin_len):
            return ""

        return veld[start_pos + begin_len:end_pos]

    return ""

@udf(returnType = StringType())
def CZ_SAS_BR_T_TELEFONIE_AANBODTYPE(vdn) -> str:
    aanbod_vdn = {"2811", "8080", "8840", "9936", "9956", "9996", "3480", "3139"}
    intern_doorverbinden_vdn = {"2110", "2115", "2120", "9905", "9935"}
    keuze2_vdn = {"2812", "2513"}
    keuze3_vdn = {"2813", "2514"}
    keuze4_vdn = {"2814", "2515"}
    vraagbaak_vdn = {"2200", "5985"}
    aanbod_vru_vdn = {"8111", "8333"}

    if vdn in aanbod_vdn:
        result = "Aanbod"
    elif vdn in intern_doorverbinden_vdn:
        result = "Intern doorverbinden"
    elif vdn in keuze2_vdn:
        result = "Keuze 2"
    elif vdn in keuze3_vdn:
        result = "Keuze 3"
    elif vdn in keuze4_vdn:
        result = "Keuze 4"
    elif vdn in vraagbaak_vdn:
        result = "Vraagbaak"
    elif vdn in aanbod_vru_vdn:
        result = "Aanbod VRU"
    else:
        result = "Overig"

    return result

# def days_act_act(start_date, end_date):
#     return (end_date - start_date).days if start_date and end_date else None
@udf(returnType = IntegerType())
def CZ_SAS_BR_T_TERMIJN_DAGEN():
    return 0

# Assuming that the function is supposed to exclude certain codes
# and return a specific flag or value for those codes.
@udf(returnType = StringType())
def CZ_SAS_BR_T_UITSLUITEN_VERGOEDING_FNT(code) -> str:
    excluded_codes = {"660", "661", "665", "989", "996", "999"}

    if code in excluded_codes:
        return "Exclude" # Replace 'Exclude' with the appropriate flag or value as per business rule

    return code

@udf(returnType = StringType())
def CZ_SAS_BR_T_UZOVI(kde_kts, kde_jet) -> str:

    if kde_kts in ("697830", "197010", "197011"):
        return "9991"

    if kde_jet == "0010":
        return "0104"

    if kde_jet == "0020":
        return "0201"

    if kde_jet == "0021":
        return "7053"

    return "7119"

@udf(returnType = StringType())
def CZ_SAS_BR_T_UZOVI_AANV_VERZ(uzovi_code: str):

    if uzovi_code == "9991":
        result = "7119"
    elif uzovi_code == "7053":
        result = "0201"
    else:
        result = uzovi_code

    return result

@udf(returnType = StringType())
def CZ_SAS_BR_T_UZOVI_OHRA_OVERIG(uzovi_code, uzovi_naam) -> str:

    if uzovi_code == "0201":
        result = "OHRA ZK"
    elif uzovi_code == "7053":
        result = "OHRA ZV"
    else:
        result = (uzovi_naam or "").strip() # strip function used to mimic SAS strip function

    return result

# cz_sas_br_t_verantwoording_dtm
# SAS Code: case when &PRO_GTB. = '010101' then &VERRICHTING_INGANG_DTM.
# else input (&PRO_GTB.||'01', yymmdd8.) end
@udf(returnType = StringType())
def CZ_SAS_BR_T_VERANTWOORDING_DTM(pro_gtb, verrichting_ingang_dtm) -> str:

    def sas_date_to_datetime(sas_date_str):
        if len(sas_date_str) == 6 and sas_date_str.isdigit():
            try:
                return datetime.datetime.strptime(sas_date_str + "01", "%y%m%d")
            except ValueError:
                return None
        else:
            return None

    if pro_gtb == "010101":
        result = str(verrichting_ingang_dtm)
    else:
        datetime_obj = sas_date_to_datetime(pro_gtb)
        result = datetime_obj.strftime("%Y-%m-%d") if datetime_obj else ""

    return result

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_VERDRAGSVERZEKERDEN(kostensoort_code) -> int:

    if kostensoort_code in ("697830", "197010", "197011"):
        return 1

    return 0

@udf(returnType = StringType())
def CZ_SAS_BR_T_VERRICHTING_SUBCODE(soortcode, stelselcode, subsoortcode):

    if soortcode in ("12", "13") and stelselcode in ("1", "2"):

        if "0" <= subsoortcode[:1] <= "9" and "A" <= subsoortcode[1:2] <= "Z":
            return subsoortcode[1:11]

        if "000" <= subsoortcode[:3] <= "999" and "A" <= subsoortcode[3:4] <= "Z":
            return subsoortcode[3:12]

        if "0000" <= subsoortcode[:4] <= "9999" and "A" <= subsoortcode[4:5] <= "Z":
            return subsoortcode[4:12]

    return subsoortcode

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_VERR_ATL_UKG(
        kde_krk,
        schaderegel_type_code,
        ukg_ivr_atl,
        kde_kts,
        kde_srt_vrt,
        ukg_uvr_bdg,
        skd_vrt,
        fnt_vgl,
):

    if kde_krk == "01" and schaderegel_type_code == "1":
        return ukg_ivr_atl if ukg_ivr_atl is not None else 0

    if (kde_kts in ("697830", "197010", "197011") and kde_srt_vrt == "01" and schaderegel_type_code == "1"):
        return ukg_ivr_atl if ukg_ivr_atl is not None else 0

    if kde_krk == "02" and schaderegel_type_code == "2":
        return 0

    if kde_krk == "02" and schaderegel_type_code == "1" and ukg_uvr_bdg > 0:
        return 1

    if kde_krk == "02" and schaderegel_type_code == "1" and ukg_uvr_bdg < 0:
        return - 1

    if (kde_kts in ("697830", "197010", "197011") and kde_srt_vrt == "02" and schaderegel_type_code == "2"):
        return 0

    if (
        kde_kts in ("697830", "197010", "197011")
        and kde_srt_vrt == "02"
        and schaderegel_type_code == "1"
        and ukg_uvr_bdg > 0
    ):
        return 1

    if (
        kde_kts in ("697830", "197010", "197011")
        and kde_srt_vrt == "02"
        and schaderegel_type_code == "1"
        and ukg_uvr_bdg < 0
    ):
        return - 1

    if kde_krk == "07" and "000000004000" <= skd_vrt <= "000000004999":
        return ukg_ivr_atl if ukg_ivr_atl is not None else 0

    if kde_krk == "07" and ukg_ivr_atl if ukg_ivr_atl is not None else True:
        return ukg_ivr_atl if ukg_ivr_atl is not None else 0

    if kde_krk == "07" and ukg_ivr_atl if ukg_ivr_atl is not None else False:
        return ukg_ivr_atl if ukg_ivr_atl is not None else 0

    if (
        kde_kts in ("697830", "197010", "197011")
        and kde_srt_vrt == "07"
        and "000000004000" <= skd_vrt <= "000000004999"
    ):
        return ukg_ivr_atl if ukg_ivr_atl is not None else 0

    if (kde_kts in ("697830", "197010", "197011") and kde_srt_vrt == "07" and ukg_ivr_atl if ukg_ivr_atl is not None else True):
        return ukg_ivr_atl if ukg_ivr_atl is not None else 0

    if (kde_kts in ("697830", "197010", "197011") and kde_srt_vrt == "07" and ukg_ivr_atl if ukg_ivr_atl is not None else False):
        return ukg_ivr_atl if ukg_ivr_atl is not None else 0

    if schaderegel_type_code == "1" and fnt_vgl not in ("617", "618", "641"):
        return ukg_ivr_atl if ukg_ivr_atl is not None else 0

    return 0

@udf(returnType = StringType())
def CZ_SAS_BR_T_VERZEKERINGSBASIS(vzl, kde_jet):

    if vzl != "52" and kde_jet in ("0004", "0010", "0020", "0021"):
        result = "hv"
    elif vzl == "52" or kde_jet == "0008":
        result = "av"
    else:
        result = "xx"

    return result

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_WANBETALER_PROCES_TYPE():
    return 0

@udf(returnType = DateType())
def CZ_SAS_BR_T_WANBET_AAN_AFMELDING_DTM(status, ttype, dtm_aanm_broninh, tijdstrip_creatie):

    if status == 4:
        if ttype == 1:
            result = dtm_aanm_broninh
        elif ttype in (2, 3):
            result = tijdstrip_creatie
        else:
            result = None
    else:
        result = None

    return result

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_WTG_KDE_IDC(wtg_kde):

    if wtg_kde not in ("0", "9", " ", "!@"):
        return 1

    return 0

# WTG_KDE    BR_T_WTG_KDE_IDC_NUM    Technical
# case when &WTG_KDE not in (0,9,.) then 1 else 0 end
# Bepaling WET_TARIEF_GEZONDHEIDSZORG_IDC op numerieke bronkolom
@udf(returnType = DoubleType())
def CZ_SAS_BR_T_WTG_KDE_IDC_NUM(wtg_kde):
    result = 0.0

    if wtg_kde is not None:
        result = 1.0 if int(wtg_kde) not in (0, 9) else 0.0
    else:
        result = 0.0

    return result

@udf(returnType = DoubleType())
def CZ_SAS_BR_T_WTG_OSL_BDG(bdg_trf_nto, trf_bto) -> float:

    if bdg_trf_nto is None or bdg_trf_nto == "":
        bdg_trf_nto_value = 0.0
    else:
        bdg_trf_nto_value = float(bdg_trf_nto)

    if trf_bto is None or trf_bto == "":
        trf_bto_value = 0.0
    else:
        trf_bto_value = float(trf_bto)

    if bdg_trf_nto_value < 0:
        return trf_bto_value * - 1

    if bdg_trf_nto_value > 0:
        return trf_bto_value * 1

    return 0.0

@udf(returnType = IntegerType())
def CZ_SAS_BR_T_ZKA_CODE_NAAR_ID(bron: str, keuzelijst: str, code: str):
    # This function concatenates
    # result = bron.strip() + "|" + keuzelijst.strip() + "|" + code.strip()
    # and then puts the result in
    # $ZKA_CODE_NAAR_ID. format determined by the _ZKA_CODELIJSTTABEL job.
    # That job makes formats based on ZKA_IL._ZKA_CODELIJSTTABEL table contents
    raise ValueError("this function may not be performed in Spark, see CZINS03-1597")

@udf(returnType = StringType())
def CZ_SAS_BR_T_ZKA_ID_NAAR_CODE() -> str:
    return ""

# Define the UDF
@udf(returnType = StringType())
def CZ_SAS_BR_T_ZKA_ID_NAAR_OMS():
    return ""

# Define the UDF for the given business rule
@udf(returnType = DoubleType())
def CZ_SAS_BR_T_ZORGKOSTEN_BASIS(
        fse_vwk_ukg,
        bdg_ukg_uvr,
        sts_ukg,
        fnt_vgl_scg,
        verzekeringsgrondslag_code,
        kostensoort_code,
        bdg_uvr_scg,
) -> float:

    if (
        (fse_vwk_ukg == 6)
        and (bdg_ukg_uvr != 0)
        and (sts_ukg == 2)
        and (fnt_vgl_scg not in (660, 661, 665, 989, 996, 999))
    ):

        if verzekeringsgrondslag_code == 11:
            return float(bdg_uvr_scg)

        if verzekeringsgrondslag_code == 12:

            if kostensoort_code not in (116560, 184220):
                return float(bdg_uvr_scg)

            return 0.0

        return 0.0

    return 0.0

@udf(returnType = StringType())
def CZ_SAS_BR_T_ZORGPRODUCT(verrichting_soort_code, verrichting_subsoort_code) -> str:

    if verrichting_soort_code == "54":
        result = verrichting_subsoort_code[3:12]
    else:
        result = ""

    return result

@udf(returnType = StringType())
def CZ_SAS_BR_T_ZVL_AGB_CODE8(zvl_agb_code):
    return "" if zvl_agb_code is None else zvl_agb_code[2:4] + zvl_agb_code[5:11]

def CZ_SAS_BR_VOORUITBET_OPENSTAAND_BDGGenerator():
    """
from decimal import Decimal, ROUND_HALF_UP 
"""
    from decimal import Decimal # pylint: disable = W0611

    @udf(returnType = DoubleType())
    def func(
            bedrag: float,
            bedrag_betaald: float,
            bedrag_verrekend: float,
            bedrag_afgeboekt: float,
    ) -> float:
        bedrag = bedrag or 0.0
        bedrag_betaald = bedrag_betaald or 0.0
        bedrag_verrekend = bedrag_verrekend or 0.0
        bedrag_afgeboekt = bedrag_afgeboekt or 0.0
        bedrag = Decimal(str(bedrag))
        bedrag_betaald = Decimal(str(bedrag_betaald))
        bedrag_verrekend = Decimal(str(bedrag_verrekend))
        bedrag_afgeboekt = Decimal(str(bedrag_afgeboekt))
        result = float(bedrag - bedrag_betaald - bedrag_verrekend - bedrag_afgeboekt)

        return result

    return func

CZ_SAS_BR_VOORUITBET_OPENSTAAND_BDG = CZ_SAS_BR_VOORUITBET_OPENSTAAND_BDGGenerator()

@udf(returnType = DoubleType())
def CZ_SAS_BR_V_ADRES(straatnaam, huisnummer, woonplaats, land_id):

    if (straatnaam is not None and huisnummer is not None and woonplaats is not None and land_id > 0.0):
        return 1.0

    return 0.0

@udf(returnType = DateType())
def CZ_SAS_DATUMS(value: str):
    # pylint: disable=import-outside-toplevel
    from datetime import datetime as dt
    result = dt.strptime("99991231", "%Y%m%d").date()
    value = "" if not value else value

    try:
        return dt.strptime(value, "%Y%m%d").date()
    except ValueError:
        pass

    try:
        return dt.strptime(value, "%d%m%Y").date()
    except ValueError:
        return result

@udf(returnType = TimestampType())
def CZ_SAS_DHMS(input_date, hours, minutes, seconds):
    # pylint: disable=import-outside-toplevel
    from datetime import date, timedelta, datetime

    try:

        if isinstance(input_date, (float, int)):
            # If input_date is a float that represents in SAS the number of
            # days since '1960-01-01'
            days = input_date
            delta = timedelta(days = days, seconds = seconds, minutes = minutes, hours = hours)
            base_date = datetime(1960, 1, 1)

            return base_date + delta

        if isinstance(input_date, date):
            # If input_date is date as ordinary date type
            days = input_date.day
            months = input_date.month
            years = input_date.year
            delta = timedelta(hours = float(hours), minutes = float(minutes), seconds = float(seconds))
            dt = datetime(years, months, days)

            return dt + delta

        # If input_date is in any other format, return None
        return None
    except Exception:
        return None

@udf(returnType = IntegerType())
def CZ_SAS_EXTRACT_MILLISECONDS(datetime_str):
    # pylint: disable=import-outside-toplevel
    from datetime import datetime
    dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S.%f")
    milliseconds = dt.microsecond // 1000

    return milliseconds

@udf(returnType = DateType())
def CZ_SAS_FMT_DDMMYY10(param):
    # pylint: disable=import-outside-toplevel
    from re import match, findall
    from datetime import date

    if param is None:
        return None

    proc = False
    result = None
    pattern = r"^(\d{1,2})[-/](\d{1,2})[-/](\d{4})\D*"

    if match(pattern, param):
        proc = True
        day, month, year = findall(pattern, param)[0]

    pattern = r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})\D*"

    if match(pattern, param):
        proc = True
        year, month, day = findall(pattern, param)[0]

    if proc:
        year = int(year)
        month = int(month)
        day = int(day)

        # if month>12: day,month = month,day
        try:
            result = date(year = year, month = month, day = day)
        except Exception as e:
            print(f"An exception occurred: {str(e)}")
            result = None

    return result

@udf(returnType = DoubleType())
def CZ_SAS_FUZZ(x):
    """UDF cz_sas_fuzz"""
    # pylint: disable=import-outside-toplevel
    import math

    if math.fabs(x) < 1.0E-12:
        return 0

    return x

@udf(returnType = StringType())
def CZ_SAS_GET_NL_DAYOFTHEWEEKNAME(day):
    days = [
"maandag", "dinsdag", "woensdag", "donderdag", "vrijdag", "zaterdag", "zondag", ]

    # Adjusting month index as Python lists are zero-based
    return days[day - 1]

@udf(returnType = StringType())
def CZ_SAS_GET_NL_MONTHNAME(month):
    months = ["januari", "februari", "maart", "april", "mei", "juni", "juli", "augustus", "september", "oktober",
              "november", "december",]

    # Adjusting month index as Python lists are zero-based
    return months[month - 1]

@udf(returnType = StringType())
def CZ_SAS_HUIDIGE_SITUATIE(date):
    return date.strftime("%Y-%m-%d")

@udf(returnType = DoubleType())
def CZ_SAS_INPUT_BEST(value: str) -> float:

    if value is None:
        return None

    try:
        return float(value[:12])
    except ValueError:
        return None

@udf(returnType = IntegerType())
def CZ_SAS_INTCK(custom_interval, start_date, end_date, method="D"):
    # pylint: disable=import-outside-toplevel
    # pylint: disable=missing-function-docstring
    import builtins
    from datetime import timedelta

    if start_date is None or end_date is None:
        return None

    custom_interval = custom_interval.upper()
    method = method.upper()

    if method in ("C", "CONTINUOUS", "CONT"):

        if custom_interval in ("DAY", "DAYS", "DTDAY"):
            result = (end_date - start_date).days

        if custom_interval in ("MONTH", "MONTHS", "DTMONTH"):
            delta_years = end_date.year - start_date.year
            delta_months = end_date.month - start_date.month
            total_months = delta_years * 12 + delta_months

            # Adjust for day differences
            if end_date.day < start_date.day:
                total_months -= 1

            # Special case for leap years
            is_leap_start = start_date.year % 4 == 0 and (start_date.year % 100 != 0 or start_date.year % 400 == 0)
            is_leap_end = end_date.year % 4 == 0 and (end_date.year % 100 != 0 or end_date.year % 400 == 0)

            if (
                start_date.strftime("%m-%d") == "02-28"
                and end_date.strftime("%m-%d") == "02-29"
                and is_leap_end
                and not is_leap_start
            ):
                total_months -= 1

            if (
                start_date.strftime("%m-%d") == "02-29"
                and end_date.strftime("%m-%d") == "02-28"
                and is_leap_start
                and not is_leap_end
            ):
                total_months += 1

            result = total_months

        if custom_interval in ("YEAR", "YEARS", "DTYEAR"):
            delta_years = end_date.year - start_date.year

            if (end_date.month, end_date.day) < (start_date.month, start_date.day):
                delta_years -= 1

            result = delta_years

    # Add other intervals here
    if method in ("D", "DESCRETE", "DISC"):

        # not fully tested yet, please use with caution
        if custom_interval in ("WEEKDAY23456W", "DTWEEKDAY23456W"):
            delta = builtins.sum(
                [
                  (start_date + timedelta(days = i)).weekday() not in (2, 3, 4, 5, 6)
                  for i in range((end_date - start_date).days + 1)
                ]
            )

            return delta

        delta = end_date - start_date

        if custom_interval in ("SECOND", "SECONDS"):
            return delta.seconds

        if custom_interval in ("MINUTE", "MINUTES"):
            return ceil(delta.seconds / 60)

        if custom_interval in ("HOUR", "HOURS"):
            return ceil(delta.seconds / (60 * 60))

        if custom_interval in ("DAY", "DAYS", "DTDAY"):
            return delta.days

        if custom_interval in ("MONTH", "MONTHS", "DTMONTH"):
            return (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)

        if custom_interval in ("YEAR", "YEARS", "DTYEAR"):
            return end_date.year - start_date.year

        raise ValueError(f"{custom_interval} interval is not currently supported")

    return result

@udf(returnType = DateType())
def CZ_SAS_INTNX(custom_interval: str, start_from, increment: int, alignment: str="BEGINNING"):
    # pylint: disable=import-outside-toplevel
    from datetime import date, timedelta, datetime
    from dateutil.relativedelta import relativedelta
    import calendar
    custom_interval = custom_interval.upper()
    alignment = alignment.upper()

    if increment is None:
        return None

    if not isinstance(start_from, date):
        if isinstance(start_from, str):
            start_from = datetime.strptime(start_from, "%Y-%m-%d").date()
        elif isinstance(start_from, datetime):
            start_from = start_from.date()
        else:
            raise ValueError(f"input argument start_from of type {type(start_from)} is not currently supported")

    if alignment in ("S", "SAME", "SAMEDAY"):

        if custom_interval in ("SECOND", "SECONDS"):
            return start_from + timedelta(seconds = increment)

        if custom_interval in ("MINUTE", "MINUTES"):
            return start_from + timedelta(minutes = increment)

        if custom_interval in ("HOUR", "HOURS"):
            return start_from + timedelta(hours = increment)

        if custom_interval in ("DAY", "DAYS", "DTDAY"):
            return start_from + timedelta(days = increment)

        if custom_interval in ("WEEK", "DTWEEK"):
            return start_from + timedelta(weeks = increment)

        if custom_interval in ("MONTH", "MONTHS", "DTMONTH"):
            return date(start_from.year, start_from.month, start_from.day) + relativedelta(months = increment)

        if custom_interval in ("QTR", "DTQTR"):
            return date(start_from.year, start_from.month, start_from.day) + relativedelta(months = increment * 3)

        if custom_interval in ("YEAR", "YEARS", "DTYEAR"):
            return date(start_from.year, start_from.month, start_from.day) + relativedelta(years = increment)

        raise ValueError(f"{custom_interval} interval for alignment {alignment} is not currently supported")

    if alignment in ("BEGINNING", "B", "BEGIN"):

        if custom_interval in ("DAY", "DAYS", "DTDAY"):
            return start_from + timedelta(days = increment)

        if custom_interval in ("WEEK", "DTWEEK"):
            return (start_from + timedelta(weeks = increment) - timedelta(days = start_from.weekday() + 1))

        if custom_interval in ("MONTH", "MONTHS", "DTMONTH"):
            res_date = date(start_from.year, start_from.month, start_from.day) + relativedelta(months = increment)

            return date(res_date.year, res_date.month, 1)

        if custom_interval in ("YEAR", "YEARS", "DTYEAR"):
            start_from = start_from.replace(month = 1).replace(day = 1)

            return start_from + relativedelta(years = increment)

        raise ValueError(f"{custom_interval} interval for alignment {alignment} is not currently supported")

    if alignment in ("END", "E"):

        if custom_interval in ("DAY", "DAYS", "DTDAY"):
            return start_from + timedelta(days = increment)

        if custom_interval in ("WEEK", "DTWEEK"):
            return (
                start_from
                + timedelta(weeks = increment)
                - timedelta(days = start_from.weekday())
                + timedelta(days = 5)
            )

        if custom_interval in ("MONTH", "MONTHS", "DTMONTH"):
            res_date = start_from + relativedelta(months = increment)

            return date(res_date.year, res_date.month, calendar.monthrange(res_date.year, res_date.month)[1], )

        if custom_interval in ("YEAR", "YEARS", "DTYEAR"):
            res_date = date(start_from.year, start_from.month, start_from.day) + relativedelta(years = increment)

            return date(res_date.year, 12, res_date.day)

        raise ValueError(f"{custom_interval} interval for alignment {alignment} is not currently supported")

    raise ValueError(f"{custom_interval} something is not currently supported")

@udf(returnType = DateType())
def CZ_SAS_INTNX_DATE(custom_interval, start_from, increment, alignment="BEGINNING"):
    # pylint: disable=import-outside-toplevel
    from datetime import date, timedelta
    from dateutil.relativedelta import relativedelta
    import calendar
    custom_interval = custom_interval.upper()
    alignment = alignment.upper()

    if increment is None:
        return None

    if not isinstance(start_from, date):
        start_from = date(start_from.year, start_from.month, start_from.day)

    if alignment in ("S", "SAME", "SAMEDAY"):

        if custom_interval in ("DAY", "DAYS", "DTDAY"):
            return start_from + timedelta(days = increment)

        if custom_interval in ("WEEK", "DTWEEK"):
            return start_from + timedelta(weeks = increment)

        if custom_interval in ("MONTH", "MONTHS", "DTMONTH"):
            return date(start_from.year, start_from.month, start_from.day) + relativedelta(months = increment)

        if custom_interval in ("QTR", "DTQTR"):
            return date(start_from.year, start_from.month, start_from.day) + relativedelta(months = increment * 3)

        if custom_interval in ("YEAR", "YEARS", "DTYEAR"):
            # return start_from + relativedelta(years=increment)
            return date(start_from.year, start_from.month, start_from.day) + relativedelta(years = increment)

        raise ValueError(f"{custom_interval} interval is not currently supported")

    if alignment in ("BEGINNING", "B"):

        if custom_interval in ("DAY", "DAYS", "DTDAY"):
            return start_from + timedelta(days = increment)

        if custom_interval in ("WEEK", "DTWEEK"):
            return (start_from + timedelta(weeks = increment) - timedelta(days = (start_from.weekday() + 1) % 7))

        if custom_interval in ("MONTH", "MONTHS", "DTMONTH"):
            res_date = date(start_from.year, start_from.month, start_from.day) + relativedelta(months = increment)

            return date(res_date.year, res_date.month, 1)

        if custom_interval in ("YEAR", "YEARS", "DTYEAR"):
            start_from = start_from.replace(month = 1).replace(day = 1)

            return start_from + relativedelta(years = increment)

        raise ValueError(f"{custom_interval} interval is not currently supported")

    if alignment in ("END", "E"):

        if custom_interval in ("DAY", "DAYS", "DTDAY"):
            return start_from + timedelta(days = increment)

        if custom_interval in ("WEEK", "DTWEEK"):
            return (
                start_from
                + timedelta(weeks = increment)
                - timedelta(days = (start_from.weekday() + 1) % 7)
                + timedelta(days = 6)
            )

        if custom_interval in ("MONTH", "MONTHS", "DTMONTH"):
            res_date = start_from + relativedelta(months = increment)

            return date(res_date.year, res_date.month, calendar.monthrange(res_date.year, res_date.month)[1], )

        if custom_interval in ("YEAR", "YEARS", "DTYEAR"):
            res_date = date(start_from.year, start_from.month, start_from.day) + relativedelta(years = increment)

            return date(res_date.year, 12, res_date.day)

        raise ValueError(f"{custom_interval} interval is not currently supported")

    raise ValueError(f"{alignment} alignment is not currently supported")

@udf(returnType = TimestampType())
def CZ_SAS_INTNX_TS(custom_interval, start_from, increment, alignment="BEGINNING"):
    # pylint: disable=import-outside-toplevel
    from datetime import datetime, date, timedelta
    from dateutil.relativedelta import relativedelta
    import calendar
    custom_interval = custom_interval.upper()
    alignment = alignment.upper()

    if not isinstance(start_from, datetime):
        start_from = datetime(start_from.year, start_from.month, start_from.day)

    if alignment in ("S", "SAME"):

        if custom_interval in ("SECOND", "SECONDS"):
            return start_from + timedelta(seconds = increment)

        if custom_interval in ("MINUTE", "MINUTES"):
            return start_from + timedelta(minutes = increment)

        if custom_interval in ("HOUR", "HOURS"):
            return start_from + timedelta(hours = increment)

        if custom_interval in ("DAY", "DAYS", "DTDAY"):
            return start_from + timedelta(days = increment)

        if custom_interval in ("WEEK", "DTWEEK"):
            return start_from + timedelta(weeks = increment)

        if custom_interval in ("MONTH", "MONTHS", "DTMONTH"):
            return date(start_from.year, start_from.month, start_from.day) + relativedelta(months = increment)

        if custom_interval in ("QTR", "DTQTR"):
            return date(start_from.year, start_from.month, start_from.day) + relativedelta(months = increment * 3)

        if custom_interval in ("YEAR", "YEARS", "DTYEAR"):
            return date(start_from.year, start_from.month, start_from.day) + relativedelta(years = increment)

        raise ValueError(f"{custom_interval} interval is not currently supported")

    if alignment in ("BEGINNING", "B"):

        if custom_interval in ("SECOND", "SECONDS"):
            return (start_from + timedelta(seconds = increment)).replace(microsecond = 0)

        if custom_interval in ("MINUTE", "MINUTES"):
            return (start_from + timedelta(minutes = increment)).replace(second = 0, microsecond = 0)

        if custom_interval in ("HOUR", "HOURS"):
            return (start_from + timedelta(hours = increment)).replace(minute = 0, second = 0, microsecond = 0)

        if custom_interval in ("DAY", "DAYS", "DTDAY"):
            return (start_from + timedelta(days = increment)).replace(hour = 0, minute = 0, second = 0, microsecond = 0)

        if custom_interval in ("WEEK", "DTWEEK"):
            return (start_from + timedelta(weeks = increment) - timedelta(days = start_from.weekday() + 1)).replace(
                hour = 0,
                minute = 0,
                second = 0,
                microsecond = 0
            )

        if custom_interval in ("MONTH", "MONTHS", "DTMONTH"):
            res_date = date(start_from.year, start_from.month, start_from.day) + relativedelta(months = increment)

            return datetime(res_date.year, res_date.month, res_date.day)\
                .replace(day = 1, hour = 0, minute = 0, second = 0, microsecond = 0)

        if custom_interval in ("YEAR", "YEARS", "DTYEAR"):
            res_date = date(start_from.year, start_from.month, start_from.day) + relativedelta(years = increment)

            return datetime(res_date.year, 1, res_date.day)\
                .replace(day = 1, hour = 0, minute = 0, second = 0, microsecond = 0)

        raise ValueError(f"{custom_interval} interval is not currently supported")

    if alignment in ("END", "E"):

        if custom_interval in ("SECOND", "SECONDS"):
            return (start_from + timedelta(seconds = increment)).replace(microsecond = 999999)

        if custom_interval in ("MINUTE", "MINUTES"):
            return (start_from + timedelta(minutes = increment)).replace(second = 59, microsecond = 999999)

        if custom_interval in ("HOUR", "HOURS"):
            return (start_from + timedelta(hours = increment)).replace(minute = 59, second = 59, microsecond = 999999)

        if custom_interval in ("DAY", "DAYS", "DTDAY"):
            return (start_from + timedelta(days = increment)).replace(
                hour = 23,
                minute = 59,
                second = 59,
                microsecond = 999999
            )

        if custom_interval in ("WEEK", "DTWEEK"):
            return (
                start_from
                + timedelta(weeks = increment)
                - timedelta(days = start_from.weekday())
                + timedelta(days = 5)
            )\
                .replace(
                hour = 23,
                minute = 59,
                second = 59,
                microsecond = 999999
            )

        if custom_interval in ("MONTH", "MONTHS", "DTMONTH"):
            res_date = date(start_from.year, start_from.month, start_from.day) + relativedelta(months = increment)

            return datetime(res_date.year, res_date.month, res_date.day)\
                .replace(day = calendar.monthrange(res_date.year, res_date.month)[1], hour = 23, minute = 59, second = 59, microsecond = 999999, )

        if custom_interval in ("YEAR", "YEARS", "DTYEAR"):
            res_date = date(start_from.year, start_from.month, start_from.day) + relativedelta(years = increment)

            return datetime(res_date.year, 12, res_date.day)\
                .replace(day = calendar.monthrange(res_date.year, 12)[1], hour = 23, minute = 59, second = 59, microsecond = 999999, )

        raise ValueError(f"{custom_interval} interval is not currently supported")

    raise ValueError(f"{alignment} alignment is not currently supported")

@udf(returnType = BooleanType())
def CZ_SAS_ISPARTNER():
    return 0

@udf(returnType = StringType())
def CZ_SAS_KIK_ID(idfield):

    if idfield is None:
        return None

    # assuming idfield can be cast to an int and formatted as zero-padded
    # string
    return f"{int(idfield):08d}"

@udf(FloatType())
def CZ_SAS_MOD(dividend, divisor):

    if dividend is None or divisor is None or divisor == 0.0:
        return None

    return dividend % divisor

# This function mimics some of the SAS PUT function behaviour.
# Some means:
# 1. It does not work with negative numbers since I never needed them.
# 2. Small numbers, like 0.000002 = 2E-6 have not been tested for the same reason.
# 3. I cannot understand the logic why SAS takes the
# "maximum meaningful digits" or the "scientific notation" approach.
# This means, put(619273619273, z9.) equals to "6.1927E11", not "6192736E5", but
#             put(123456.1,     z5.) equals to "123E3",     not "1.2E5"
# moreover,   put(123456789012, z6.) equals to "0123E9",    not "1234E8", nor "1.2E12"
# Why?
# No ideas.
@udf(returnType = StringType())
def CZ_SAS_PUT_Z(input_number: float, w: int, d: int=0) -> str:

    if input_number is None:
        return ".".rjust(w)

    if d != 0:  # fail fast
        raise ValueError(f"""put(value, z{w}.{d}) is not currently """ + """supported, zX.0 formats only""")

    # rounding because Python builtin round() is insane:
    # round(100.5) == 100 and round(101.5) == 102
    if input_number % 1 >= 0.5:
        input_number = int(input_number) + 1
    else:
        input_number = int(input_number)

    if len(str(input_number)) <= w:
        return str(input_number).zfill(w)

    w_adjusted: int = w

    if input_number >= float("1E10"):
        # exponent will require extra char
        # so we have less space for mantissa digits
        w_adjusted = w_adjusted - 1

    sci_notation = format(input_number, f".{w_adjusted - 4}E")
    # Split the scientific notation into mantissa and exponent
    mantissa, exponent = sci_notation.split("E")
    # squash "1.000" for mantissa to "1" and "+02" for exponent to "2"
    exponent = int(exponent)
    mantissa = float(mantissa)
    formatted_number = f"{mantissa}E{exponent}"

    if len(formatted_number) < w:
        formatted_number = formatted_number.zfill(w)

    return formatted_number

@udf(returnType = StringType())
def CZ_SAS_RELATIE_DECRYPT(decrypt_var: str, relatienr_var: str, is_bsn: bool) -> str:
    # helper variables
    numbers1_26 = list(range(1, 27))
    numbers1_10 = list(range(1, 10))
    numbers_coded = ["3", "7", "9", "6", "1", "8", "5", "2", "4"]
    uc_letters = ["P", "B", "I", "Y", "E", "O", "V", "S", "G", "C", "R", "W", "L", "T", "A", "N", "K", "D", "X", "Z",
                  "M", "F", "U", "J", "Q", "H",]
    lc_letters = ["t", "x", "b", "p", "z", "a", "i", "d", "n", "w", "k", "s", "q", "y", "g", "o", "u", "e", "m", "c",
                  "h", "r", "v", "f", "l", "j",]
    # ucl_to_numbers
    h_hog_eltrs = dict(zip(uc_letters, numbers1_26))
    # numbers_to_ucl
    h_hog_eloc = dict(zip(numbers1_26, uc_letters))
    # lcl_to_numbers
    h_lag_eltrs = dict(zip(lc_letters, numbers1_26))
    # numbers_to_lcl
    h_lag_eloc = dict(zip(numbers1_26, lc_letters))
    # numbers_to_coded
    h_cfr_sloc = dict(zip(numbers1_10, numbers_coded))
    # coded numbers to numbers
    h_cfr_sltrs = dict(zip(numbers_coded, numbers1_10))
    retain_dict = {}
    retain_dict["bsn_temp"] = 0
    decrypted_string = ""

    def numbers_decrypt(letter: str) -> str:
        rc_cfr_sltrs = h_cfr_sltrs.get(letter)

        if rc_cfr_sltrs is not None:
            cfr2pos = 9 if i_sas % 9 == 0 else i_sas % 9
            cfr2 = int(relatienr_var[cfr2pos - 1])
            cfr3 = rc_cfr_sltrs - 3 if cfr2 in [0, 9] else rc_cfr_sltrs - cfr2
            cfr3 = cfr3 + 9 if cfr3 < 1 else cfr3
            rc_cfr_sloc = h_cfr_sloc.get(cfr3)

            if rc_cfr_sloc is not None:
                return rc_cfr_sloc

        raise ValueError(f"Invalid number: {letter}")

    def bsn_decrypt(letter: str) -> str:

        if i_sas == 1:
            retain_dict["bsn_temp"] = int(letter) * 9
        elif i_sas < 9:
            retain_dict["bsn_temp"] = retain_dict["bsn_temp"] + int(letter) * (10 - i_sas)
        elif i_sas == 9:
            mod_bsn_temp = retain_dict["bsn_temp"] % 11
            formatted_mod = f"{mod_bsn_temp:02}"
            out = formatted_mod[- 1]

            return str(out)

        return letter

    def ucl_decrypt(letter: str) -> str:
        rc_hog_eltrs = h_hog_eltrs.get(letter)

        if rc_hog_eltrs is not None:
            cfr2pos = 9 if i_sas % 9 == 0 else i_sas % 9
            cfr2 = int(relatienr_var[cfr2pos - 1])
            cfr3 = rc_hog_eltrs - 4 if cfr2 == 0 else rc_hog_eltrs - cfr2
            cfr3 = cfr3 + 26 if cfr3 < 1 else cfr3
            rc_hog_eloc = h_hog_eloc.get(cfr3)

            if rc_hog_eloc is not None:
                return rc_hog_eloc

        raise ValueError(f"Invalid letter: {letter}")

    def lcl_decrypt(letter: str) -> str:
        rc_lag_eltrs = h_lag_eltrs.get(letter)

        if rc_lag_eltrs is not None:
            cfr2pos = 9 if i_sas % 9 == 0 else i_sas % 9
            cfr2 = int(relatienr_var[cfr2pos - 1])
            cfr3 = rc_lag_eltrs - 4 if cfr2 == 0 else rc_lag_eltrs - cfr2
            cfr3 = cfr3 + 26 if cfr3 < 1 else cfr3
            rc_lag_eloc = h_lag_eloc.get(cfr3)

            if rc_lag_eloc is not None:
                return rc_lag_eloc

        raise ValueError(f"Invalid letter: {letter}")

    # if decrypt_var is null
    if decrypt_var is None:
        return None

    # if decrypt_var just empty string or string with spaces only return None
    if decrypt_var.strip() == "":
        return None

    if relatienr_var is None:
        return decrypt_var

    for i_sas, one_letter in enumerate(decrypt_var, start = 1):
        asci_l = ord(one_letter)
        tmp = one_letter

        if 49 <= asci_l <= 57:
            tmp = numbers_decrypt(one_letter)

        if is_bsn:
            tmp2 = bsn_decrypt(tmp)

            if i_sas == 9:
                tmp = tmp2
        elif 65 <= asci_l <= 90:
            tmp = ucl_decrypt(one_letter)
        elif 97 <= asci_l <= 122:
            tmp = lcl_decrypt(one_letter)

        decrypted_string += tmp

    return decrypted_string

@udf(returnType = StringType())
def CZ_SAS_RESOLVE_PATTERN(
        items,
        pattern,
        dlm,
        token_string = "#"
):
    # pylint: disable=import-outside-toplevel
    import re

    try:

        # Check if either items or pattern is empty - if not: provide a
        # description for the function
        if not items or not pattern:
            print("\nRESOLVEPATTERN> Generates text by substituting values of a list into a pattern")
            print("        ")
            print("   Syntax: resolvePattern(item_list, pattern, delimiter, token_string)")
            print("        - item_list:      Item list")
            print("        - delimiter:      List delimiter")
            print("        - pattern:        Pattern for substitution")
            print("        - token_string:   Token for substitution")
            print()

            return ""

        # Convert item_list to a string representation
        if isinstance(items, list):
            item_list_str = " ".join(str(item) if item is not None else "" for item in items)
        else:
            item_list_str = items

        # Split item_list using delimiter
        list_dlm = item_list_str.split("/")[1].strip() if "/" in item_list_str else " "
        item_list_str = " ".join(item_list_str.split("/")[0].split())
        # Remove leading and trailing spaces from delimiter and token_string
        dlm = dlm.strip()
        token_string = token_string.strip()
        # If delimiter is empty, set it to a single space
        dlm = " " if not dlm else dlm
        # Substitute tokens in pattern with items
        resolved_pattern = ""

        for item in item_list_str.split(list_dlm):
            if item:

                if resolved_pattern:
                    resolved_pattern += dlm

                resolved_pattern += re.sub(re.escape(token_string), item, pattern)

        return resolved_pattern
    except Exception as e:
        print("An error occurred:", str(e))

        return ""

# pylint: disable=unused-argument
@udf(returnType = StringType())
def CZ_SAS_SCAN(input_string, position, delimiter=" !$%&()*+,-./;<^|", modifiers=None):
    # pylint: disable=import-outside-toplevel
    import re

    if not input_string:
        return ""

    # Split the input_string using the delimiter
    words = re.split(delimiter, input_string)

    # Handle negative position to extract from the right
    if position < 0:
        position = len(words) + position + 1

    # Extract the word at the specified position
    if 1 <= position <= len(words):
        return words[position - 1]

    # Return an empty string if position is out of range
    return ""

def CZ_SAS_SETSASVERWIJDERDATUMGenerator():
    """ This import is done here 
because when this code is assembled in Prophecy, for strange reason
importing datetime only in code made the pipeline crash.
This file is an essential part of UDF and should not be treated separately.
"""
    from datetime import datetime # pylint: disable = W0611

    @udf(returnType = TimestampType())
    def func(
            SASVerwijderDatumTijd: datetime,  # pylint: disable = C0103
            sysprocesdatum: datetime,
    ) -> datetime:

        if SASVerwijderDatumTijd is None:
            return None

        return sysprocesdatum

    return func

CZ_SAS_SETSASVERWIJDERDATUM = CZ_SAS_SETSASVERWIJDERDATUMGenerator()

@udf(returnType = IntegerType())
def CZ_SAS_TEST(value: int):
    return value * value

@udf(returnType = DoubleType())
def CZ_SAS_TIMEPART(param1):
    return (param1.hour * 3600 + param1.minute * 60 + param1.second + param1.microsecond * 1.0E-6)

@udf(returnType = StringType())
def CZ_SAS_TR_AANMANING_ZWAARTE_OMS() -> str:
    return "AANMANING_ZWAARTE"

@udf(returnType = StringType())
def CZ_SAS_TR_AFBOEKING_REDEN_OMS(type_oms):

    if type_oms == "AFBOEKING_REDEN":
        return "AFBOEKING_REDEN"

    return type_oms

@udf(returnType = StringType())
def CZ_SAS_TR_BETAALWIJZE_OMS():
    return "BETAALWIJZE"

@udf(returnType = StringType())
def CZ_SAS_TR_BETALINGSREGELING_STATUS_OMS() -> str:
    return "BETALINGSREGELING_STATUS"

@udf(returnType = StringType())
def CZ_SAS_TR_BETALINGSREG_EINDE_REDEN_OMS() -> str:
    return "BETALINGSREG_EINDE_REDEN"

@udf(returnType = StringType())
def CZ_SAS_TR_BETALINGSVERKEER_STATUS_OMS(type_oms) -> str:
    return "BETALINGSVERKEER_STATUS" if type_oms == "BETALINGSVERKEER_STATUS" else ""

@udf(returnType = StringType())
def CZ_SAS_TR_BETALINGSVERKEER_SUBTYPE(betalingsverkeer_subtype) -> str:

    if betalingsverkeer_subtype == 1:
        return "002"

    return "001"

@udf(returnType = StringType())
def CZ_SAS_TR_BOEKING_STATUS_OMS(type_oms) -> str:
    return "BOEKING_STATUS" if type_oms == "BOEKERING_STATUS" else type_oms

@udf(returnType = StringType())
def CZ_SAS_TR_DC_NOTA_STATUS_OMS(type_oms):

    if type_oms == "DC_NOTA_STATUS":
        result = "DC_NOTA_STATUS"
    else:
        result = type_oms

    return result

@udf(returnType = StringType())
def CZ_SAS_TR_DEBITEURCREDITEUR_SRT_OMS():
    return "DEBITEURCREDITEUR_SRT"

@udf(returnType = StringType())
def CZ_SAS_TR_DEBITEURCREDITEUR_STATUS_OMS(type_oms):

    if type_oms == "DEBITEURCREDITEUR_STATUS":
        result = "DEBITEURCREDITEUR_STATUS"
    else:
        result = type_oms

    return result

@udf(returnType = StringType())
def CZ_SAS_TR_INCASSOPROCESSTAP_OMS(type_oms) -> str:
    return "INCASSOPROCESSTAP" if type_oms is not None else ""

@udf(returnType = StringType())
def CZ_SAS_TR_INCASSO_DOSSIER_STATUS_OMS(type_oms):

    if type_oms == "INCASSO_DOSSIER_STATUS":
        return "INCASSO_DOSSIER_STATUS"

    return type_oms

@udf(returnType = StringType())
def CZ_SAS_TR_INCASSO_FASE_OMS(type_oms):

    if type_oms == "INCASSO_FASE":
        return type_oms

    return None # or return another value as per your business rule for cases not matching 'INCASSO_FASE'

@udf(returnType = StringType())
def CZ_SAS_TR_KREDIETTERMIJN_CATEGORIE_OMS(type_oms) -> str:

    if type_oms == "KREDIETTERMIJN_CATEGORIE":
        return "KREDIETTERMIJN_CATEGORIE"

    return type_oms

# paramkolom    TR_MARKEER_ONTBREKENDE_REF_ID    Technical
# case when &paramkolom. is missing then -2 else &paramkolom. end
# Markeer waarde met -2 als er onterecht een ontbrekende waarde in bron zit
@udf(returnType = DoubleType())
def CZ_SAS_TR_MARKEER_ONTBREKENDE_REF_ID(paramkolom) -> float:

    if paramkolom is None:
        result = - 2.0
    else:
        result = float(paramkolom)

    return result

# paramkolom    TR_MARKEER_REF_ID_NVT    Technical
# case when &paramkolom. is missing then -3 else &paramkolom. end
# Markeer waarde met -3 als er een ontbrekende waarde in de bron zit en
# dit ook mag
@udf(returnType = DoubleType())
def CZ_SAS_TR_MARKEER_REF_ID_NVT(paramkolom):
    """UDF cz_sas_tr_markeer_ref_id_nvt"""

    if paramkolom is None:
        return - 3.0

    return float(paramkolom)

@udf(returnType = StringType())
def CZ_SAS_TR_OPHOUDING_SCHADE_OMS(type_oms) -> str:
    return "OPHOUDING_SCHADE" if type_oms == "OPHOUDING_SCHADE" else type_oms

@udf(returnType = StringType())
def CZ_SAS_TR_OUDERDOM_CATEGORIE_OMS():
    return "OUDERDOM_CATEGORIE"

# Business Rule: Assign fixed string value 'SCHULDSANERING_SITUATIE' to
# the column
@udf(returnType = StringType())
def CZ_SAS_TR_SCHULDSANERING_SITUATIE_OMS() -> str:
    return "SCHULDSANERING_SITUATIE"

@udf(returnType = StringType())
def CZ_SAS_TR_STORNERING_SOORT_OMS(type_oms):
    return "STORNERING_SOORT" if type_oms == "STORNERING_SOORT" else type_oms

# Name: cz_sas_tr_stornering_status_oms. SAS Code: type_oms =
# 'STORNERING_STATUS'
@udf(returnType = StringType())
def CZ_SAS_TR_STORNERING_STATUS_OMS(type_oms) -> str:
    return "STORNERING_STATUS" if type_oms == "STORNERING_STATUS" else type_oms

@udf(returnType = DoubleType())
def CZ_SAS_TR_SUM(paramkolom):
    """UDF cz_sas_tr_sum"""
    # pylint: disable=import-outside-toplevel
    import math

    if paramkolom is None:
        return 0.0

    if math.fabs(paramkolom * 100) < 1.0E-12:
        return 0.0

    return math.fsum(paramkolom * 100) / 100

@udf(returnType = StringType())
def CZ_SAS_TR_UITVAL_REDEN_OMS(type_oms) -> str:

    if type_oms == "UITVAL_REDEN":
        result = "UITVAL_REDEN"
    else:
        result = type_oms

    return result

@udf(returnType = StringType())
def CZ_SAS_TR_VORDERING_STATUS_OMS(type_oms):

    if type_oms == "VORDERING_STATUS":
        result = "VORDERING_STATUS"
    else:
        result = type_oms

    return result

@udf(returnType = StringType())
def CZ_SAS_TR_WANBETALER_AANKOND_TYPE_OMS() -> str:
    return "WANBETALER_AANKOND_TYPE"

# Business rule: Set type_oms to 'WANBETALER_PROCES_TYPE'
@udf(returnType = StringType())
def CZ_SAS_TR_WANBETALER_PROCES_TYPE_OMS() -> str:
    return "WANBETALER_PROCES_TYPE"

@udf(returnType = StringType())
def CZ_SAS_TR_WANBET_AANAFMELD_REDEN_OMS():
    return "WANBET_AANAFMELD_REDEN"

@udf(returnType = StringType())
def CZ_SAS_TR_WANBET_AANAFMELD_STATUS_OMS():
    return "WANBET_AANAFMELD_STATUS"

@udf(returnType = StringType())
def CZ_SAS_TR_WANBET_AANAFMELD_TYPE_OMS():
    return "WANBET_AANAFMELD_TYPE"

@udf(returnType = StringType())
def CZ_SAS_TR_WERKAANBOD_TYPE_OMS():
    return "WERKAANBOD_TYPE"

@udf(returnType = StringType())
def CZ_SAS_TR_WERKOPDRACHT_STATUS_OMS():
    return "WERKOPDRACHT_STATUS"

# cz_sas_tv_sublabel_niet_aanwezig
# case when SUBLABEL = 0 then . when SUBLABEL ^= 0 then SUBLABEL end
# Indien SUBLABEL gelijk is aan 0, return None (missing), anders return
# SUBLABEL
@udf(returnType = DoubleType())
def CZ_SAS_TV_SUBLABEL_NIET_AANWEZIG(sublabel) -> float:

    if sublabel == 0:
        return 0.0

    return float(sublabel) if sublabel is not None else 0.0

@udf(returnType = StringType())
def CZ_SAS_UPPER(value: str):

    if value is None:
        return None

    transformations = {
        # 'ß': 'ß', 'ÿ': 'ÿ' in Unicode representation
        chr(223): chr(223),  # 'ß' to 'ß'
        chr(255): chr(255),  # 'ÿ' to 'ÿ'
    }

    return "".join([transformations.get(c, c.upper()) for c in value])

@udf(returnType = DateType())
def CZ_SAS_VORIGE_SITUATIE(datum):
    # pylint: disable=import-outside-toplevel
    from datetime import timedelta

    return datum - timedelta(days = 1)

@udf(returnType = DateType())
def CZ_SAS_YYYYMMD(int_field, min_date="1960-01-01"):
    # pylint: disable=import-outside-toplevel
    from datetime import date, timedelta
    min_date = date.fromisoformat(min_date)

    return min_date + timedelta(days = int_field)

# Business Rule: Convert a string in the format 'YYMMDD' to a Date type
@udf(returnType = DateType())
def CZ_SAS_YYYYMMDD2DATE(datestr):

    if datestr is None:
        return None

    try:
        return datetime.strptime(datestr, "%y%m%d").date()
    except ValueError:
        return None # or you can handle the exception as per the requirement
