from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, aa: str=None):
        self.spark = None
        self.update(aa)

    def update(self, aa: str="a"):
        self.aa = aa
        pass
