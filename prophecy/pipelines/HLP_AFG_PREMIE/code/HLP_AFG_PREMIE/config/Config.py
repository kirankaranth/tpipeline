from HLP_AFG_PREMIE.graph.Loop.config.Config import SubgraphConfig as Loop_Config
from HLP_AFG_PREMIE.graph.Loop_1.config.Config import SubgraphConfig as Loop_1_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            SYSPROCESDATUM: str=None,
            SYSPEILDATUM: str=None,
            JOB_CURRENT_DATE: str=None,
            ETLS_JOBNAME: str=None,
            SNAPSHOT_FROM_HISTORY_MAX_VALUE: str=None,
            read_env: str=None,
            write_env: str=None,
            Loop: dict=None,
            Loop_1: dict=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            SYSPROCESDATUM, 
            SYSPEILDATUM, 
            JOB_CURRENT_DATE, 
            ETLS_JOBNAME, 
            SNAPSHOT_FROM_HISTORY_MAX_VALUE, 
            read_env, 
            write_env, 
            Loop, 
            Loop_1
        )

    def update(
            self,
            SYSPROCESDATUM: str="CAST('2024-11-27 14:55:25Z' AS TIMESTAMP)",
            SYSPEILDATUM: str="CAST('2024-11-27' AS DATE)",
            JOB_CURRENT_DATE: str="CAST('2024-11-27' AS DATE)",
            ETLS_JOBNAME: str="HLP_AFG_PREMIE",
            SNAPSHOT_FROM_HISTORY_MAX_VALUE: str="",
            read_env: str="migration",
            write_env: str="migration",
            Loop: dict={},
            Loop_1: dict={},
            **kwargs
    ):
        prophecy_spark = self.spark
        self.SYSPROCESDATUM = SYSPROCESDATUM
        self.SYSPEILDATUM = SYSPEILDATUM
        self.JOB_CURRENT_DATE = JOB_CURRENT_DATE
        self.ETLS_JOBNAME = ETLS_JOBNAME
        self.SNAPSHOT_FROM_HISTORY_MAX_VALUE = SNAPSHOT_FROM_HISTORY_MAX_VALUE
        self.read_env = read_env
        self.write_env = write_env
        self.Loop = self.get_config_object(
            prophecy_spark, 
            Loop_Config(prophecy_spark = prophecy_spark), 
            Loop, 
            Loop_Config
        )
        self.Loop_1 = self.get_config_object(
            prophecy_spark, 
            Loop_1_Config(prophecy_spark = prophecy_spark), 
            Loop_1, 
            Loop_1_Config
        )
        pass
