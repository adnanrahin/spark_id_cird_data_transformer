package org.data.transformer.extractor

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.data.transformer.writer.DataFileWriterLocal

class DataExtractor(personDomainDf: DataFrame) {

  def findAllMalePerson(): DataFrame = {
    val maleDf = personDomainDf.filter(lower(col("gender")) === "male")
    maleDf.persist(StorageLevel.MEMORY_AND_DISK)
    maleDf
  }

  def countTotalIidEachState(): DataFrame = {
    val resultDf = personDomainDf.groupBy(col("state")).count()
    resultDf.persist(StorageLevel.MEMORY_AND_DISK)
    resultDf
  }

  def topCitiesByPopulation(topN: Int = 10): DataFrame = {
    val cityPopulationDf = personDomainDf.groupBy(col("city")).count().alias("population")
    val windowSpec = Window.orderBy(col("count").desc)
    val rankedCitiesDf = cityPopulationDf.withColumn("rank", row_number().over(windowSpec))
    val topCitiesDf = rankedCitiesDf.filter(col("rank") <= topN)
    topCitiesDf.persist(StorageLevel.MEMORY_AND_DISK)
    topCitiesDf
  }

  def findPersonsWithInvalidEmails(): DataFrame = {
    val invalidEmailDf = personDomainDf.filter(
      !col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
    )
    invalidEmailDf.persist(StorageLevel.MEMORY_AND_DISK)
    invalidEmailDf
  }

  def stateWiseMaleFemaleCount(): DataFrame = {
    val maleFemaleCountDf = personDomainDf.groupBy("state", "gender").count().alias("count")
    val pivotDf = maleFemaleCountDf.groupBy("state").pivot("gender").sum("count")
    pivotDf.persist(StorageLevel.MEMORY_AND_DISK)
    pivotDf
  }

  def topStatesByPersons(topN: Int = 5): DataFrame = {
    val stateCountDf = personDomainDf.groupBy("state").count().alias("total_persons")
    val topStatesDf = stateCountDf.orderBy(col("count").desc).limit(topN)
    topStatesDf.persist(StorageLevel.MEMORY_AND_DISK)
    topStatesDf
  }

  def countUniqueIpsPerState(): DataFrame = {
    val ipCountsDf = personDomainDf.groupBy("state").agg(
      countDistinct(col("ipV4")).alias("unique_ipV4_count"),
      countDistinct(col("ipV6")).alias("unique_ipV6_count")
    )
    ipCountsDf.persist(StorageLevel.MEMORY_AND_DISK)
    ipCountsDf
  }

  def findPersonsWithValidEmails(): DataFrame = {
    val validEmailDf = personDomainDf.filter(
      col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
    )
    validEmailDf.persist(StorageLevel.MEMORY_AND_DISK)
    validEmailDf
  }

  def cityWiseGenderDistribution(): DataFrame = {
    val genderCountDf = personDomainDf.groupBy("city", "gender").count().alias("gender_count")
    val pivotDf = genderCountDf.groupBy("city").pivot("gender").sum("count")
    pivotDf.persist(StorageLevel.MEMORY_AND_DISK)
    pivotDf
  }

  def findPeopleUnderSamePublicIp4(): DataFrame = {
    // Group by ipV4 column and count distinct guIds (assuming guId is a unique identifier)
    val peopleUnderSameIp4Df = personDomainDf.groupBy("ipV4").agg(
      countDistinct(col("guId")).alias("unique_persons_count")
    ).filter(col("unique_persons_count") > 1) // Filter to get only those with more than one person

    peopleUnderSameIp4Df.persist(StorageLevel.MEMORY_AND_DISK)
    peopleUnderSameIp4Df
  }

  def generateAllExtractsAndStore(dataPath: String): Unit = {
    val maleDf = findAllMalePerson()
    DataFileWriterLocal.dataWriterParquet(maleDf, dataPath, "find_all_male_person")

    val totalIidDf = countTotalIidEachState()
    DataFileWriterLocal.dataWriterParquet(totalIidDf, dataPath, "count_total_iid_each_state")

    val topCitiesDf = topCitiesByPopulation()
    DataFileWriterLocal.dataWriterParquet(topCitiesDf, dataPath, "top_cities_by_population")

    val invalidEmailsDf = findPersonsWithInvalidEmails()
    DataFileWriterLocal.dataWriterParquet(invalidEmailsDf, dataPath, "find_persons_with_invalid_emails")

    val stateWiseGenderCountDf = stateWiseMaleFemaleCount()
    DataFileWriterLocal.dataWriterParquet(stateWiseGenderCountDf, dataPath, "statewise_male_female_count")

    val topStatesDf = topStatesByPersons()
    DataFileWriterLocal.dataWriterParquet(topStatesDf, dataPath, "top_states_by_persons")

    val uniqueIpsDf = countUniqueIpsPerState()
    DataFileWriterLocal.dataWriterParquet(uniqueIpsDf, dataPath, "count_unique_ips_per_state")

    val validEmailsDf = findPersonsWithValidEmails()
    DataFileWriterLocal.dataWriterParquet(validEmailsDf, dataPath, "find_persons_with_valid_emails")

    val cityWiseGenderDistDf = cityWiseGenderDistribution()
    DataFileWriterLocal.dataWriterParquet(cityWiseGenderDistDf, dataPath, "citywise_gender_distribution")

    val peopleUnderSameIp4Df = findPeopleUnderSamePublicIp4()
    DataFileWriterLocal.dataWriterParquet(peopleUnderSameIp4Df, dataPath, "find_people_under_same_public_ip4")
  }

}
