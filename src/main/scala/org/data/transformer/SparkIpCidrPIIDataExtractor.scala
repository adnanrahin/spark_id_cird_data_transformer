package org.data.transformer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data.transformer.domain_loader.IpCidrCustomDomainUserDataLoader
import org.data.transformer.extractor.DataExtractor

object SparkIpCidrPIIDataExtractor {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkIpCidrPIIDataExtractor")
      .master("local[*]") // Comment out if running in local standalone cluster
      .getOrCreate()

    val sc = spark.sparkContext

    val inputSourceDataDir = args(0)
    val extractOutputDir = args(1)


    val personDomainDataLoader: IpCidrCustomDomainUserDataLoader =
      new IpCidrCustomDomainUserDataLoader(inputSourceDataDir, spark)

    val personDomainDf = personDomainDataLoader.loadDf()

    val extractor = new DataExtractor(personDomainDf)
    extractor.generateAllExtractsAndStore(extractOutputDir)

  }
}
