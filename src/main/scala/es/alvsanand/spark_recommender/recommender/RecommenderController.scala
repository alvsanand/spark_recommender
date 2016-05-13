package es.alvsanand.spark_recommender.recommender

import akka.actor.ActorSystem
import com.mongodb.casbah.Imports._
import es.alvsanand.spark_recommender.model.{ProductRecommendationRequest, Recommendation, UserRecommendationRequest}
import es.alvsanand.spark_recommender.utils.{ESConfig, MongoConfig}
import spray.httpx.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, NullOptions}
import spray.routing.SimpleRoutingApp


object RecommenderControllerProtocol extends DefaultJsonProtocol with NullOptions with SprayJsonSupport {
  implicit val productRecommendationRequestFormat = jsonFormat1(ProductRecommendationRequest)
  implicit val userRecommendationRequestFormat = jsonFormat1(UserRecommendationRequest)
  implicit val recommendationFormat = jsonFormat2(Recommendation)
}

/**
  * Created by asantos on 11/05/16.
  */
object RecommenderController extends SimpleRoutingApp {

  import RecommenderControllerProtocol._

  implicit val system = ActorSystem("ActorSystem")

  def run(serverPort: Int)(implicit mongoConf: MongoConfig, esConf: ESConfig): Unit = {
    implicit val mongoClient = MongoClient(MongoClientURI("mongodb://%s".format(mongoConf.hosts.split(";").mkString(","))))

    startServer(interface = "localhost", port = serverPort) {
      path("recs" / "cf" / "product") {
        post(
          entity(as[ProductRecommendationRequest]) { request =>
            // invoke using curl -H "Content-Type: application/json" -X POST -d @book-sample.json http://localhost:8080/books.json
            // use book-sample.json from src/main/resources
            complete {
              RecommenderService.getCollaborativeFilteringRecommendations(request).toStream
            }
          }
        )
      } ~
        path("recs" / "cf" / "user") {
          post(
            entity(as[UserRecommendationRequest]) { request =>
              complete {
                RecommenderService.getCollaborativeFilteringRecommendations(request).toStream
              }
            }
          )
        }
    }
  }
}
