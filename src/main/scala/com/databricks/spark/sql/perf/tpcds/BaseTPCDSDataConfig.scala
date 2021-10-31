package com.databricks.spark.sql.perf.tpcds

case class BaseTPCDSDataConfig(
  scaleFactor: Int = 1,
  format: String = "parquet",
  useDoubleForDecimal: Boolean = false,
  useStringForDate: Boolean = false,
  filterOutNullPartitionValues: Boolean = false,
  clusterByPartitionColumns: Boolean = true,
  dbPrefix: String = ""
)

trait WithBaseTPCDSDataConfig {
  def baseConfig: BaseTPCDSDataConfig
}

object BaseTPCDSDataConfig {
  private val defaultBaseConfig = BaseTPCDSDataConfig()

  def parseConfig[T <: WithBaseTPCDSDataConfig](
      parser: scopt.OptionParser[T], updater: (BaseTPCDSDataConfig, T) => T): Unit = {

    parser.opt[Int]("scaleFactor")
      .action((x, c) => updater(c.baseConfig.copy(scaleFactor = x), c))
      .valueName(defaultBaseConfig.scaleFactor.toString)
      .text("scaleFactor defines the size of the dataset to generate (in GB)")

    parser.opt[String]("format")
      .action((x, c) => updater(c.baseConfig.copy(format = x), c))
      .valueName(defaultBaseConfig.format)
      .text("valid spark format, Parquet, ORC ...")

    parser.opt[Boolean]("useDoubleForDecimal")
      .action((x, c) => updater(c.baseConfig.copy(useDoubleForDecimal = x), c))
      .valueName(defaultBaseConfig.useDoubleForDecimal.toString)
      .text("true to replace DecimalType with DoubleType")

    parser.opt[Boolean]("useStringForDate")
      .action((x, c) => updater(c.baseConfig.copy(useStringForDate = x), c))
      .valueName(defaultBaseConfig.useStringForDate.toString)
      .text("true to replace DateType with StringType")

    parser.opt[Boolean]("filterOutNullPartitionValues")
      .action((x, c) => updater(c.baseConfig.copy(filterOutNullPartitionValues = x), c))
      .valueName(defaultBaseConfig.filterOutNullPartitionValues.toString)
      .text("true to filter out the partition with NULL key value")

    parser.opt[Boolean]("clusterByPartitionColumns")
      .action((x, c) => updater(c.baseConfig.copy(clusterByPartitionColumns = x), c))
      .valueName(defaultBaseConfig.clusterByPartitionColumns.toString)
      .text("shuffle to get partitions coalesced into single files")

    parser.opt[String]("dbPrefix")
        .action((x, c) => updater(c.baseConfig.copy(dbPrefix = x), c))
        .valueName(defaultBaseConfig.dbPrefix)
        .text("the prefix of the result DB name")
  }

  def buildDBName(config: BaseTPCDSDataConfig): String = {
    s"${config.dbPrefix}tpcds_sf${config.scaleFactor}" +
        s"""_${if (config.useDoubleForDecimal) "no" else "with"}decimal""" +
        s"""_${if (config.useStringForDate) "no" else "with"}date""" +
        s"""_${if (config.filterOutNullPartitionValues) "no" else "with"}nulls"""
  }
}
