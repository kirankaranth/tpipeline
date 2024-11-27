from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(self, prophecy_spark=None, NUMMER: float=0.0, **kwargs):
        self.NUMMER = NUMMER
        pass

    def update(self, updated_config):
        self.NUMMER = updated_config.NUMMER
        pass

Config = SubgraphConfig()
