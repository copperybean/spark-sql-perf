package org.apache.spark.tpcds

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration

import scala.util.Random

case class DistCpConfig(
    srcPath: String = null,
    destPath: String = null
)

object DistCp {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[DistCpConfig]("DistCp") {
      opt[String]('s', "srcPath")
          .required()
          .action { (x, c) => c.copy(srcPath = x)}
          .text("the source path to copy")
      opt[String]('d', "destPath")
          .required()
          .action { (x, c) => c.copy(destPath = x)}
          .text("the destination path")
    }
    parser.parse(args, DistCpConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def cpFile(srcPath: String, destPath: String, conf: Broadcast[SerializableConfiguration])(
      srcFile: String) : String = {
    val srcFSPath = new Path(srcPath)
    val destFSPath = new Path(destPath)

    val srcFS = srcFSPath.getFileSystem(conf.value.value)
    val destFS = destFSPath.getFileSystem(conf.value.value)

    val fileSrcFSPath = new Path(srcFile)
    val fileDestFSPath = if (fileSrcFSPath.equals(srcFSPath)) {
      destFSPath
    } else {
      new Path(destFSPath, fileSrcFSPath.toString.substring(srcFSPath.toString.length + 1))
    }

    val fileDestParent = fileDestFSPath.getParent
    if (!destFS.exists(fileDestParent)) {
      destFS.mkdirs(fileDestParent)
    }

    val in = srcFS.open(fileSrcFSPath)
    val out = destFS.create(fileDestFSPath, true)
    IOUtils.copyBytes(in, out, 1024 * 1024, true)

    fileDestFSPath.toString
  }

  def getAllFiles(fsPaths: Array[Path], fs: FileSystem) : Array[String] = {
    val status = fs.listStatus(fsPaths)

    val filePaths = status.filter(!_.isDirectory).map(_.getPath.toString)
    val dirFSPaths = status.filter(_.isDirectory).map(_.getPath)

    if (dirFSPaths.isEmpty) {
      filePaths
    } else {
      filePaths ++ getAllFiles(dirFSPaths, fs)
    }
  }

  private def run(config: DistCpConfig): Unit = {
    val spark = SparkSession.builder.appName("Distcp").getOrCreate()

    val srcFSPath = new Path(config.srcPath)
    val files = getAllFiles(Array(srcFSPath), srcFSPath.getFileSystem(spark.sparkContext.hadoopConfiguration))
    val randomFiles = Random.shuffle(files.toList)
    val broadcastConf = spark.sparkContext.broadcast(
      new SerializableConfiguration(spark.sparkContext.hadoopConfiguration))
    val srcPath = srcFSPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
        .getFileStatus(srcFSPath).getPath.toString

    val num = spark.sparkContext
        .parallelize(randomFiles)
        .map(cpFile(srcPath, config.destPath, broadcastConf))
        .count

    println("========================================")
    println(s"$num files copied")
    println("========================================")
  }
}
