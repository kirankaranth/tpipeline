package io.prophecy.pipelines.automatedexternalpipclone.graph

import io.prophecy.libs._
import io.prophecy.pipelines.automatedexternalpipclone.config.ConfigStore._
import io.prophecy.pipelines.automatedexternalpipclone.config.Context
import io.prophecy.pipelines.automatedexternalpipclone.udfs.UDFs._
import io.prophecy.pipelines.automatedexternalpipclone.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object OrderBy_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(concat(col("first_name"), col("last_name")).asc)

}
