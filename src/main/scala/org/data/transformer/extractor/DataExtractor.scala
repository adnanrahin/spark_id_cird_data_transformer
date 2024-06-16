package org.data.transformer.extractor

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.data.transformer.writer.DataFileWriterLocal
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{currentMirror => cm}
import scala.util.{Try, Success, Failure}


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
      !col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"))
    invalidEmailDf.persist(StorageLevel.MEMORY_AND_DISK)
    invalidEmailDf
  }

  def statewiseMaleFemaleCount(): DataFrame = {
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
    val peopleUnderSameIp4Df = personDomainDf.groupBy("ipV4").agg(
      countDistinct(col("guId")).alias("unique_persons_count")
    ).filter(col("unique_persons_count") > 1)

    peopleUnderSameIp4Df.persist(StorageLevel.MEMORY_AND_DISK)
    peopleUnderSameIp4Df
  }


  def generateAllExtractsAndStore(dataPath: String): Unit = {
    val instanceMirror = cm.reflect(this)
    val methods = typeOf[DataExtractor].decls.collect {
      case m: MethodSymbol if m.returnType =:= typeOf[DataFrame] && m.isPublic => m
    }

    methods.foreach { method =>
      val methodName = method.name.toString
      Try {
        val result = instanceMirror.reflectMethod(method).apply().asInstanceOf[DataFrame]
        DataFileWriterLocal.dataWriterParquet(result, dataPath, methodName)
        println(s"Successfully wrote DataFrame from $methodName to $dataPath/$methodName")
      } match {
        case Success(_) => // Do nothing, successfully wrote the DataFrame
        case Failure(exception) =>
          println(s"Failed to write DataFrame from method $methodName: ${exception.getMessage}")
          exception.printStackTrace()
      }
    }
  }
}
