/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf.tpcds

import org.apache.spark.sql.SparkSession

case class GenTPCDSDataConfig(
    baseConfig: BaseTPCDSDataConfig = BaseTPCDSDataConfig(),
    dsdgenDir: String = "/opt/tpcds-kit/tools",
    overwrite: Boolean = false,
    analyzeTables: Boolean = true,
    partitionTables: Seq[String] = Seq("inventory", "web_returns", "catalog_returns", "store_returns", "web_sales", "catalog_sales", "store_sales"),
    noPartitionedTables: Seq[String] = Seq("call_center", "catalog_page", "customer", "customer_address", "customer_demographics", "date_dim", "household_demographics", "income_band", "item", "promotion", "reason", "ship_mode", "store", "time_dim", "warehouse", "web_page", "web_site")
) extends WithBaseTPCDSDataConfig

/**
 * Gen TPCDS data.
 */
object GenTPCDSData {
  val defaultConf = GenTPCDSDataConfig()

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[GenTPCDSDataConfig]("Generate TPC-DS Data") {
      BaseTPCDSDataConfig.parseConfig[GenTPCDSDataConfig](this, (baseUpdated, conf) => {
        conf.copy(baseConfig = baseUpdated)
      })

      opt[String]('d', "dsdgenDir")
        .action { (x, c) => c.copy(dsdgenDir = x) }
        .valueName(defaultConf.dsdgenDir)
        .text("location of dsdgen")
      opt[Boolean]('o', "overwrite")
        .action((x, c) => c.copy(overwrite = x))
        .valueName(defaultConf.overwrite.toString)
        .text("overwrite the data that is already there")
      opt[Boolean]('a', "analyze")
        .action((x, c) => c.copy(analyzeTables = x))
        .valueName(defaultConf.analyzeTables.toString)
        .text("analyze the tables to generate statistic information")
      opt[Seq[String]]('p', "partitionTables")
        .action((x, c) => c.copy(partitionTables = x))
        .valueName(defaultConf.partitionTables.mkString(","))
        .text("the partitioned tables, \"all\" means all tables")
      opt[Seq[String]]('n', "noPartitionedTables")
        .action((x, c) => c.copy(noPartitionedTables = x))
        .valueName(defaultConf.noPartitionedTables.mkString(","))
        .text("the no partitioned tables,  \"all\" means all tables")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, defaultConf) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: GenTPCDSDataConfig) {
    val spark = SparkSession
      .builder()
      .appName(s"Generate TPCDS Data with scale: ${config.baseConfig.scaleFactor}")
      // Limit the memory used by parquet writer
      .config("spark.hadoop.parquet.memory.pool.ratio", "0.1")
      // Compress with snappy:
      .config("spark.sql.parquet.compression.codec", "snappy")
      // Don't write too huge files.
      .config("spark.sql.files.maxRecordsPerFile", "20000000")
      // TPCDS has around 2000 dates.
      .config("spark.sql.shuffle.partitions", Math.min(2000, config.baseConfig.scaleFactor * 10))
      .getOrCreate()

    val baseConf = config.baseConfig
    val rootDir = s"${baseConf.location}/tpcds/sf${baseConf.scaleFactor}-${baseConf.format}" +
        s"/useDecimal=${!baseConf.useDoubleForDecimal},useDate=${!baseConf.useStringForDate}," +
        s"filterNull=${baseConf.filterOutNullPartitionValues}"

    val tables = new TPCDSTables(spark.sqlContext,
      dsdgenDir = config.dsdgenDir,
      scaleFactor = baseConf.scaleFactor + "",
      useDoubleForDecimal = baseConf.useDoubleForDecimal,
      useStringForDate = baseConf.useStringForDate)

    val allTableNames = defaultConf.partitionTables ++ defaultConf.noPartitionedTables
    def getTableNames(tables: Seq[String]): Seq[String] = {
      if (tables.contains("all")) {
        allTableNames
      } else {
        val ret = tables.filter(_.nonEmpty)
        val invalids = ret.diff(allTableNames)
        if (invalids.nonEmpty) {
          throw new Exception("invalid table names: " + invalids.mkString(","))
        }
        ret
      }
    }
    val noPartitionedTables = getTableNames(config.noPartitionedTables)
    val partitionTables = getTableNames(config.partitionTables)
    val duplicatedTables = noPartitionedTables.intersect(partitionTables)
    if (duplicatedTables.nonEmpty) {
      throw new Exception(duplicatedTables.mkString(",") +
        "are duplicated between partitioned and non partitioned tables")
    }

    // currently, partitioned and no partitioned tables are same logic
    noPartitionedTables.foreach { t =>
      tables.genData(
        location = rootDir,
        format = baseConf.format,
        overwrite = config.overwrite,
        partitionTables = false,
        clusterByPartitionColumns = baseConf.clusterByPartitionColumns,
        filterOutNullPartitionValues = baseConf.filterOutNullPartitionValues,
        tableFilter = t,
        numPartitions = 10)
      println(s"Data of no partitioned table $t generated")
    }
    println("Done generating non partitioned tables.")

    partitionTables.foreach { t =>
      tables.genData(
        location = rootDir,
        format = baseConf.format,
        overwrite = config.overwrite,
        partitionTables = true,
        clusterByPartitionColumns = baseConf.clusterByPartitionColumns,
        filterOutNullPartitionValues = baseConf.filterOutNullPartitionValues,
        tableFilter = t,
        numPartitions = baseConf.scaleFactor * 2)
      println(s"Data of partitioned table $t generated")
    }
    println("Done generating partitioned tables.")

    val databaseName = baseConf.buildDBName

    spark.sql(s"drop database if exists $databaseName cascade")
    spark.sql(s"create database $databaseName")
    spark.sql(s"use $databaseName")

    noPartitionedTables.foreach { t =>
      tables.createExternalTables(
        rootDir,
        baseConf.format,
        databaseName,
        false,
        overwrite = config.overwrite,
        discoverPartitions = true,
        tableFilter = t)
      println(s"No partitioned table $t created")
    }
    println("Done creating no partitioned tables")
    partitionTables.foreach { t =>
      tables.createExternalTables(
        rootDir,
        baseConf.format,
        databaseName,
        true,
        overwrite = config.overwrite,
        discoverPartitions = true,
        tableFilter = t)
      println(s"Partitioned table $t created")
    }
    println("Done creating partitioned tables")

    if (config.analyzeTables) {
      (noPartitionedTables ++ partitionTables).foreach { t =>
        tables.analyzeTables(databaseName, analyzeColumns = true, tableFilter = t)
        println(s"Analyze table $t finished")
      }
      println("Done analyzing tables")
    }
  }
}
