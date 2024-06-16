package org.data.transformer.data_writer

import org.apache.spark.sql.DataFrame

object DataFileWriterLocal {

  def dataWriterParquet(dataFrame: DataFrame, dataPath: String, directoryName: String): Unit = {
    val destinationDirectory = s"$dataPath/$directoryName"
    dataFrame.write.mode("overwrite").parquet(destinationDirectory)
  }

  def dataWriterCsv(dataFrame: DataFrame, dataPath: String, directoryName: String): Unit = {
    val destinationDirectory = s"$dataPath/$directoryName"
    dataFrame.write.mode("overwrite").csv(destinationDirectory)
  }

  def dataWriterJson(dataFrame: DataFrame, dataPath: String, directoryName: String): Unit = {
    val destinationDirectory = s"$dataPath/$directoryName"
    dataFrame.write.mode("overwrite").json(destinationDirectory)
  }
}
