package io.prophecy.pipelines.testp1.config

import io.prophecy.pipelines.testp1.config.ConfigStore._
import pureconfig._
import io.prophecy.libs._
case class Config(hi: String = "a") extends ConfigBase
