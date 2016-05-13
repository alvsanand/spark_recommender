package es.alvsanand.spark_recommender

import es.alvsanand.spark_recommender.parser.{DatasetDownloader, DatasetIngestion}
import es.alvsanand.spark_recommender.utils.{ESConfig, MongoConfig}
import org.apache.spark.SparkConf
import scopt.OptionParser

/**
  * @author ${user.name}
  */
object DatasetLoaderApp extends App {

  override def main(args: Array[String]) {
    val defaultParams = scala.collection.mutable.Map[String, String]()
    defaultParams += "spark.cores" -> "local[*]"
    defaultParams += "mongo.hosts" -> "127.0.0.1:27017"
    defaultParams += "mongo.db" -> "spark_recommender"
    defaultParams += "es.hosts" -> "127.0.0.1:9200"
    defaultParams += "es.index" -> "spark_recommender"
    defaultParams += "dataset.tmp.dir" -> "%s/.spark_recommender".format(sys.env("HOME"))

    val parser = new OptionParser[scala.collection.mutable.Map[String, String]]("ScaleDataset") {
      head("Spark Recommender Example")
      opt[String]("spark.cores")
        .text("Number of cores in the Spark cluster")
        .action((x: String, c) => {
          c += "spark.cores" -> x
        })
      opt[String]("mongo.hosts")
        .text("Mongo Hosts")
        .action((x: String, c) => {
          c += "mongo.hosts" -> x
        })
      opt[String]("mongo.db")
        .text("Mongo Database")
        .action((x: String, c) => {
          c += "mongo.db" -> x
        })
      opt[String]("es.hosts")
        .text("ElasicSearch Hosts")
        .action((x: String, c) => {
          c += "es.hosts" -> x
        })
      opt[String]("es.index")
        .text("ElasicSearch index")
        .action((x: String, c) => {
          c += "es.index" -> x
        })
      opt[String]("dataset.tmp.dir")
        .text("Temporal directory to store the products dataset")
        .action((x: String, c) => {
          c += "dataset.tmp.dir" -> x
        })
    }
    parser.parse(args, defaultParams).map { params =>
      run(params.toMap)
    } getOrElse {
      System.exit(1)
    }
  }

  private def run(params: Map[String, String]): Unit = {
    implicit val conf = new SparkConf().setAppName("RecommenderTrainerApp").setMaster(params("spark.cores"))
    implicit val mongoConf = new MongoConfig(params("mongo.hosts"), params("mongo.db"))
    implicit val esConf = new ESConfig(params("es.hosts"), params("mongo.db"))


    try {
      DatasetDownloader.download(params("dataset.tmp.dir"))
      DatasetIngestion.storeData(DatasetDownloader.getFinalDstName(params("dataset.tmp.dir")))
    }
    catch {
      case e: Exception =>
        println("Error executing DatasetLoaderApp")
        println(e)
        sys.exit(1)
    }

    sys.exit(0)
  }
}
