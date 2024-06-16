package org.data.transformer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data.transformer.domain_loader.IpCidrCustomDomainUserDataLoader

object SparkIpCidrPIIDataExtractor {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkIpCidrPIIDataExtractor")
      //.master("local[*]") // Comment out if running in local standalone cluster
      .getOrCreate()

    val sc = spark.sparkContext

    val dataSourcePath = args(0)
    val dataPath = args(1)


    val personDomainDataLoader: IpCidrCustomDomainUserDataLoader =
      new IpCidrCustomDomainUserDataLoader(dataSourcePath, spark)

    val df = personDomainDataLoader.loadDf()

    df.show()

  }
}
