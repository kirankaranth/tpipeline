package io.prophecy.pipelines.automatedexternalpipclone.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
