from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            NUMMER: float=0.0,
            LAATSTE_BRN_PERSOON_ID: str="",
            EERSTE_BRN_PERSOON_ID: str="",
            **kwargs
    ):
        self.NUMMER = NUMMER
        self.LAATSTE_BRN_PERSOON_ID = LAATSTE_BRN_PERSOON_ID
        self.EERSTE_BRN_PERSOON_ID = EERSTE_BRN_PERSOON_ID
        pass

    def update(self, updated_config):
        self.NUMMER = updated_config.NUMMER
        self.LAATSTE_BRN_PERSOON_ID = updated_config.LAATSTE_BRN_PERSOON_ID
        self.EERSTE_BRN_PERSOON_ID = updated_config.EERSTE_BRN_PERSOON_ID
        pass

Config = SubgraphConfig()
