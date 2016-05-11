package es.alvsanand.spark_recommender

import akka.actor.ActorSystem
import es.alvsanand.spark_recommender.model.{ProductRecommendationRequest, UserRecommendationRequest}
import es.alvsanand.spark_recommender.parser.{DatasetDownloader, DatasetIngestion}
import es.alvsanand.spark_recommender.recommender.RecsService
import es.alvsanand.spark_recommender.recommender.RecsControllerProtocol._
import es.alvsanand.spark_recommender.utils.{ESConfig, MongoConfig}
import org.apache.spark.SparkConf
import scopt.OptionParser
import spray.httpx.SprayJsonSupport._
import spray.routing.SimpleRoutingApp

/**
  * Created by asantos on 11/05/16.
  */
object RecommenderServerApp extends App with SimpleRoutingApp {
  implicit val system = ActorSystem("ActorSystem")

  override def main(args: Array[String]) {
    val defaultParams = scala.collection.mutable.Map[String, String]()
    defaultParams += "server.port" -> "8080"
    defaultParams += "mongo.hosts" -> "127.0.0.1:27017"
    defaultParams += "mongo.db" -> "spark_recommender"
    defaultParams += "es.hosts" -> "127.0.0.1:9200"
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
    }
    parser.parse(args, defaultParams).map { params =>
      run(params.toMap)
    } getOrElse {
      System.exit(1)
    }
  }

  private def run(params: Map[String, String]): Unit = {
    val serverPort = params("server.port").toInt

    val mongoConf = new MongoConfig(params("mongo.hosts"), params("mongo.db"))
    val esConf = new ESConfig(params("es.hosts"), params("mongo.db"))


    try {
      startServer(interface = "localhost", port = serverPort) {
        path("recs/cf/product") {
          get(
            entity(as[ProductRecommendationRequest]) { request =>
              // invoke using curl -H "Content-Type: application/json" -X POST -d @book-sample.json http://localhost:8080/books.json
              // use book-sample.json from src/main/resources
              complete {
                RecsService.getCollaborativeFilteringRecommendations(request)
              }
            }
          )
        } ~
        path("recs/cf/user") {
          get(
            entity(as[UserRecommendationRequest]) { request =>
              complete {
                RecsService.getCollaborativeFilteringRecommendations(request)
              }
            }
          )
        }
      }
    }
    catch {
      case e: Exception =>
        println("Error executing DatasetLoaderApp")
        println(e)
        sys.exit(0)
    }

    sys.exit(0)
  }
}
