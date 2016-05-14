package es.alvsanand.spark_recommender.recommender

import java.net.InetAddress

import akka.actor.ActorSystem
import com.mongodb.casbah.Imports._
import es.alvsanand.spark_recommender.model.{ProductRecommendationRequest, Recommendation, SearchRecommendationRequest, UserRecommendationRequest}
import es.alvsanand.spark_recommender.parser.DatasetIngestion
import es.alvsanand.spark_recommender.utils.{ESConfig, Logging, MongoConfig}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import spray.httpx.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, NullOptions}
import spray.routing.SimpleRoutingApp


object RecommenderControllerProtocol extends DefaultJsonProtocol with NullOptions with SprayJsonSupport {
  implicit val productRecommendationRequestFormat = jsonFormat1(ProductRecommendationRequest)
  implicit val userRecommendationRequestFormat = jsonFormat1(UserRecommendationRequest)
  implicit val searchRecommendationRequestFormat = jsonFormat1(SearchRecommendationRequest)
  implicit val recommendationFormat = jsonFormat2(Recommendation)
}

/**
  * Created by asantos on 11/05/16.
  */
object RecommenderController extends SimpleRoutingApp with Logging{

  import RecommenderControllerProtocol._

  implicit val system = ActorSystem("ActorSystem")

  def run(serverPort: Int)(implicit mongoConf: MongoConfig, esConf: ESConfig): Unit = {
    implicit val mongoClient = MongoClient(MongoClientURI("mongodb://%s".format(mongoConf.hosts.split(";").mkString(","))))
    implicit val esClient = TransportClient.builder().build()
    esConf.transportHosts.split(";").foreach { case DatasetIngestion.ES_HOST_PORT_REGEX(host: String, port: String) => esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt)) }

    logger.info("Launching REST serves[port=%d]".format(serverPort))

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
      } ~
      path("recs" / "cb" / "mrl") {
        post(
          entity(as[ProductRecommendationRequest]) { request =>
            complete {
              RecommenderService.getContentBasedMoreLikeThisRecommendations(request).toStream
            }
          }
        )
      } ~
      path("recs" / "cb" / "sch") {
        post(
          entity(as[SearchRecommendationRequest]) { request =>
            complete {
              RecommenderService.getContentBasedSearchRecommendations(request).toStream
            }
          }
        )
      }
    }
  }
}
