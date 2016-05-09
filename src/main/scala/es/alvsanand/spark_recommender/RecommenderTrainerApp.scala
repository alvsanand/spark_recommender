package es.alvsanand.spark_recommender

import es.alvsanand.spark_recommender.parser.{DatasetDownloader, ProductParser}
import org.apache.spark.SparkConf
import scopt.OptionParser

/**
 * @author ${user.name}
 */
object RecommenderTrainerApp {

  def main(args : Array[String]) {
    val defaultParams = scala.collection.mutable.Map[String, String]()
    defaultParams += "spark.cores" -> "local[*]"
    defaultParams += "mongo.hosts" -> "127.0.0.1:27017"
    defaultParams += "mongo.db" -> "spark_recommender"
    defaultParams += "mongo.db" -> "spark_recommender"
    defaultParams += "es.host" -> "127.0.0.1:9200"
    defaultParams += "es.index" -> "spark_recommender"
    defaultParams += "dataset.tmp.dir" -> "%s/.spark_recommender".format(sys.env("HOME"))

    val parser = new OptionParser[scala.collection.mutable.Map[String, String]]("ScaleDataset"){
      head("Spark Recommender Example")
      opt[String]("spark.cores")
        .text("Number of cores in the Spark cluster")
        .action((x: String, c) => { c += "spark.cores" -> x })
      opt[String]("mongo.hosts")
        .text("Mongo Hosts")
        .action((x: String, c) => { c += "mongo.hosts" -> x })
      opt[String]("mongo.db")
        .text("Mongo Database")
        .action((x: String, c) => { c += "mongo.db" -> x })
      opt[String]("es.host")
        .text("ElasicSearch Host")
        .action((x: String, c) => { c += "es.host" -> x })
      opt[String]("es.index")
        .text("ElasicSearch index")
        .action((x: String, c) => { c += "es.index" -> x })
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
    implicit val conf = new SparkConf().setAppName("RecommenderTrainerApp").setMaster(params("spark.cores"))
    val mongoHosts = params("mongo.hosts")
    val mongoDB = params("mongo.db")
    val esHost = params("es.host")
    val esIndex = params("mongo.db")
    
    DatasetDownloader.download(params("dataset.tmp.dir"))
    ProductParser.storeData(DatasetDownloader.getFinalDstName(params("dataset.tmp.dir")), mongoHosts, mongoDB, esHost, esIndex)

    sys.exit(0)
  }
}
