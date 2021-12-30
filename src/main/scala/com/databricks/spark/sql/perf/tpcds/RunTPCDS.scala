package com.databricks.spark.sql.perf.tpcds

import org.apache.spark.sql.SparkSession
import scopt.OptionParser

case class RunTPCDSConfig(
    baseConfig: BaseTPCDSDataConfig = BaseTPCDSDataConfig(),
    iterations: Int = 2,
    timeoutHours: Int = 20,
    queryFilter: Seq[String] = Seq("q1", "q3"),
    runInRandom: Boolean = false
) extends WithBaseTPCDSDataConfig

object RunTPCDS {
  val defaultConf = RunTPCDSConfig()

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[RunTPCDSConfig]("Run TPC-DS Data") {
      BaseTPCDSDataConfig.parseConfig[RunTPCDSConfig](this, (baseUpdated, conf) => {
        conf.copy(baseConfig = baseUpdated)
      })

      opt[Int]('i', "iterations")
        .action { (x, c) => c.copy(iterations = x) }
        .valueName(defaultConf.iterations.toString)
        .text("iterations number to run TPCDS queries")
      opt[Int]('t', "timeoutHours")
        .action { (x, c) => c.copy(timeoutHours = x) }
        .valueName(defaultConf.timeoutHours.toString)
        .text("timeout in hours of all the queries duration")
      opt[Seq[String]]('q', "queryFilter")
        .action((x, c) => c.copy(queryFilter = x))
        .valueName(defaultConf.queryFilter.mkString(","))
        .text("the queries to run")
      opt[Boolean]('r', "runInRandom")
        .action((x, c) => c.copy(runInRandom = x))
        .valueName(defaultConf.runInRandom.toString)
        .text("run queries in a random order")
    }

    parser.parse(args, defaultConf) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: RunTPCDSConfig) {
    val spark = SparkSession.builder
      .appName("Run TPCDS")
      .enableHiveSupport()
      .getOrCreate()

    val resultLocation = s"${config.baseConfig.location}/tpcds/results"
    val databaseName = config.baseConfig.buildDBName
    spark.sql(s"use $databaseName")


    val tpcds = new TPCDS (sqlContext = spark.sqlContext)
    def queries = {
      val filtered_queries = config.queryFilter match {
        case Seq() => tpcds.tpcds2_4Queries
        case _ => tpcds.tpcds2_4Queries.filter(q => config.queryFilter.contains(q.name.split("-")(0)))
      }
      if (config.runInRandom) scala.util.Random.shuffle(filtered_queries) else filtered_queries
    }
    val tags = Map(
      "runtype" -> "benchmark",
      "database" -> databaseName,
      "scale_factor" -> (config.baseConfig.scaleFactor + ""))
    val experiment = tpcds.runExperiment(
      queries,
      iterations = config.iterations,
      resultLocation = resultLocation,
      tags = tags)

    println(experiment.toString)
    experiment.waitForFinish(config.timeoutHours * 60 * 60)

    println(experiment.html)

    import org.apache.spark.sql.functions.{col, lit, substring}
    val summary = experiment.getCurrentResults
        .withColumn("Name", substring(col("name"), 2, 100))
        .withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
        .select("Name", "Runtime")

    summary.show(10000, false)
  }
}
