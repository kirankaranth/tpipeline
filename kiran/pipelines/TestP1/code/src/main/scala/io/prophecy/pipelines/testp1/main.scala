package io.prophecy.pipelines.testp1

import io.prophecy.libs._
import io.prophecy.pipelines.testp1.config.ConfigStore._
import io.prophecy.pipelines.testp1.config._
import io.prophecy.pipelines.testp1.udfs.UDFs._
import io.prophecy.pipelines.testp1.udfs._
import io.prophecy.pipelines.testp1.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {
  def apply(spark: SparkSession): Unit = {}

  def main(args:   Array[String]): Unit = {
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    spark.conf.set("prophecy.metadata.pipeline.uri", "3252/pipelines/TestP1")
    MetricsCollector.start(spark,                    "3252/pipelines/TestP1")
    apply(spark)
    MetricsCollector.end(spark)
  }

}
