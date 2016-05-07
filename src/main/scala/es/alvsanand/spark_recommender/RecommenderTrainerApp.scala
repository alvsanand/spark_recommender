package es.alvsanand.spark_recommender

import es.alvsanand.spark_recommender.parser.DatasetDownloader
import scopt.OptionParser

/**
 * @author ${user.name}
 */
object RecommenderTrainerApp {

  def main(args : Array[String]) {
    val defaultParams = scala.collection.mutable.Map[String, String]()
    defaultParams += "spark.cores" -> "*"
    defaultParams += "cassandra.host" -> "127.0.0.1"
    defaultParams += "cassandra.keyspaceName" -> "spark_recommender"
    defaultParams += "dataset.tmp.dir" -> "%s/.spark_recommender".format(sys.env("HOME"))

    val parser = new OptionParser[scala.collection.mutable.Map[String, String]]("ScaleDataset"){
      head("Spark Recommender Example")
      opt[String]("spark.cores")
        .text("Number of cores in the Spark cluster")
        .action((x: String, c) => { c += "spark.cores" -> x })
      opt[String]("cassandra.host")
        .text("Cassandra Host")
        .action((x: String, c) => { c += "cassandra.host" -> x })
      opt[String]("cassandra.keyspaceName")
        .text("Cassandra Keyspace Name")
        .action((x: String, c) => { c += "cassandra.keyspaceName" -> x })
      opt[String]("dataset.tmp.dir")
        .text("Temporal directory to store the products dataset")
        .action((x: String, c) => { c += "dataset.tmp.dir" -> x })
    }
    parser.parse(args, defaultParams).map { params =>
      run(params.toMap)
    } getOrElse {
      System.exit(1)
    }
  }

  private def run(params: Map[String, String]): Unit = {
    DatasetDownloader.download(params("dataset.tmp.dir"))
  }
}
