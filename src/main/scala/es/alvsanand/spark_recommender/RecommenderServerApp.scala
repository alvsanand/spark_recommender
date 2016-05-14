package es.alvsanand.spark_recommender

import es.alvsanand.spark_recommender.recommender.RecommenderController
import es.alvsanand.spark_recommender.utils.{ESConfig, Logging, MongoConfig}
import scopt.OptionParser

/**
  * Created by asantos on 11/05/16.
  */
object RecommenderServerApp extends App with Logging{
  override def main(args: Array[String]) {
    val defaultParams = scala.collection.mutable.Map[String, String]()
    defaultParams += "server.port" -> "8080"
    defaultParams += "mongo.hosts" -> "127.0.0.1:27017"
    defaultParams += "mongo.db" -> "spark_recommender"
    defaultParams += "es.httpHosts" -> "127.0.0.1:9200"
    defaultParams += "es.transportHosts" -> "127.0.0.1:9300"
    defaultParams += "es.index" -> "spark_recommender"
    defaultParams += "server.port" -> "8080"

    val parser = new OptionParser[scala.collection.mutable.Map[String, String]]("ScaleDataset") {
      head("Spark Recommender Example")
      opt[String]("server.port")
        .text("HTTP server port")
        .action((x: String, c) => {
          c += "server.port" -> x
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
      opt[String]("es.httpHosts")
        .text("ElasicSearch HTTP Hosts")
        .action((x: String, c) => {
          c += "es.httpHosts" -> x
        })
      opt[String]("es.transportHosts")
        .text("ElasicSearch Transport Hosts")
        .action((x: String, c) => {
          c += "es.transportHosts" -> x
        })
      opt[String]("es.index")
        .text("ElasicSearch index")
        .action((x: String, c) => {
          c += "es.index" -> x
        })
    }
    parser.parse(args, defaultParams).map { params =>
      run(params.toMap)
    } getOrElse {
      System.exit(1)
    }
  }

  private def run(params: Map[String, String]): Unit = {
    val serverPort = params("server.port").toInt

    implicit val mongoConf = new MongoConfig(params("mongo.hosts"), params("mongo.db"))
    implicit val esConf = new ESConfig(params("es.httpHosts"), params("es.transportHosts"), params("es.index"))

    try {
      RecommenderController.run(serverPort)
    }
    catch {
      case e: Exception =>
        logger.error("Error executing RecommenderServerApp", e)
        sys.exit(1)
    }
  }
}
