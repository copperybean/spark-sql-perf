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
    location: String = "/mnt/performance-datasets",
    overwrite: Boolean = false,
    analyzeTables: Boolean = true,
    partitionTables: Seq[String] = Seq("inventory", "web_returns", "catalog_returns", "store_returns", "web_sales", "catalog_sales", "store_sales"),
    noPartitionedTables: Seq[String] = Seq("call_center", "catalog_page", "customer", "customer_address", "customer_demographics", "date_dim", "household_demographics", "income_band", "item", "promotion", "reason", "ship_mode", "store", "time_dim", "warehouse", "web_page", "web_site")
) extends WithBaseTPCDSDataConfig

/**
 * Gen TPCDS data.
 */
object GenTPCDSData {
  def main(args: Array[String]): Unit = {
    val defaultConf = GenTPCDSDataConfig()

    val parser = new scopt.OptionParser[GenTPCDSDataConfig]("Gen-TPC-DS-data") {
      BaseTPCDSDataConfig.parseConfig[GenTPCDSDataConfig](this, (baseUpdated, conf) => {
        conf.copy(baseConfig = baseUpdated)
      })

      opt[String]('d', "dsdgenDir")
        .action { (x, c) => c.copy(dsdgenDir = x) }
        .valueName(defaultConf.dsdgenDir)
        .text("location of dsdgen")
      opt[String]('l', "location")
        .action((x, c) => c.copy(location = x))
        .valueName(defaultConf.location)
        .text("root directory of location to create data in")
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
        .text("create the partitioned fact tables")
      opt[Seq[String]]('n', "noPartitionedTables")
        .action((x, c) => c.copy(noPartitionedTables = x))
        .valueName(defaultConf.noPartitionedTables.mkString(","))
        .text("\"\" means generate all tables")
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

    val rootDir = s"${config.location}/tpcds/sf${config.baseConfig.scaleFactor}-${config.baseConfig.format}" +
        s"/useDecimal=${!config.baseConfig.useDoubleForDecimal},useDate=${!config.baseConfig.useStringForDate}," +
        s"filterNull=${config.baseConfig.filterOutNullPartitionValues}"

    val tables = new TPCDSTables(spark.sqlContext,
      dsdgenDir = config.dsdgenDir,
      scaleFactor = config.baseConfig.scaleFactor + "",
      useDoubleForDecimal = config.baseConfig.useDoubleForDecimal,
      useStringForDate = config.baseConfig.useStringForDate)

    // currently, partitioned and no partitioned tables are same logic
    config.noPartitionedTables.filter(_.nonEmpty).foreach { t =>
      tables.genData(
        location = rootDir,
        format = config.baseConfig.format,
        overwrite = config.overwrite,
        partitionTables = false,
        clusterByPartitionColumns = config.baseConfig.clusterByPartitionColumns,
        filterOutNullPartitionValues = config.baseConfig.filterOutNullPartitionValues,
        tableFilter = t,
        numPartitions = 10)
    }
    println("Done generating non partitioned tables.")

    config.partitionTables.filter(_.nonEmpty).foreach { t =>
      tables.genData(
        location = rootDir,
        format = config.baseConfig.format,
        overwrite = config.overwrite,
        partitionTables = true,
        clusterByPartitionColumns = config.baseConfig.clusterByPartitionColumns,
        filterOutNullPartitionValues = config.baseConfig.filterOutNullPartitionValues,
        tableFilter = t,
        numPartitions = config.baseConfig.scaleFactor * 2)
    }
    println("Done generating partitioned tables.")

    val databaseName = BaseTPCDSDataConfig.buildDBName(config.baseConfig)

    spark.sql(s"drop database if exists $databaseName cascade")
    spark.sql(s"create database $databaseName")
    spark.sql(s"use $databaseName")

    val filteredTables = config.partitionTables.filter(_.nonEmpty) ++ config.noPartitionedTables.filter(_.nonEmpty)
    filteredTables.foreach { t =>
      tables.createExternalTables(
        rootDir,
        config.baseConfig.format,
        databaseName,
        overwrite = config.overwrite,
        discoverPartitions = true,
        tableFilter = t)
    }

    if (config.analyzeTables) {
      filteredTables.foreach { t =>
        tables.analyzeTables(databaseName, analyzeColumns = true, tableFilter = t)
      }
    }
  }
}
