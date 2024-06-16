package org.data.transformer.domain_loader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField, StringType}

class IpCidrCustomDomainUserDataLoader(filePath: String, spark: SparkSession) {

  def loadDf(): DataFrame = {
    val schema = StructType(Array(
      StructField("firstName", StringType, true),
      StructField("lastName", StringType, true),
      StructField("email", StringType, true),
      StructField("gender", StringType, true),
      StructField("ipV4", StringType, true),
      StructField("ipV6", StringType, true),
      StructField("address", StringType, true),
      StructField("state", StringType, true),
      StructField("city", StringType, true),
      StructField("longitude", StringType, true),
      StructField("latitude", StringType, true),
      StructField("guId", StringType, true),
      StructField("ipV4Cidr", StringType, true),
      StructField("ipV6Cidr", StringType, true)
    ))

    val personDomainDf = spark.read
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(schema)
      .parquet(filePath)

    personDomainDf
  }
}
