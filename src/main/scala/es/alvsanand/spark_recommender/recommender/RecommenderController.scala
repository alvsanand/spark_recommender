package es.alvsanand.spark_recommender.recommender

import java.net.InetAddress

import akka.actor.ActorSystem
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import es.alvsanand.spark_recommender.model._
import es.alvsanand.spark_recommender.utils.{ESConfig, Logging, MongoConfig}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import spray.httpx.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, NullOptions}
import spray.routing.SimpleRoutingApp


object RecommenderControllerProtocol extends DefaultJsonProtocol with NullOptions with SprayJsonSupport {
  implicit val productRecommendationRequestFormat = jsonFormat1(ProductRecommendationRequest)
  implicit val userRecommendationRequestFormat = jsonFormat1(UserRecommendationRequest)
  implicit val searchRecommendationRequestFormat = jsonFormat1(SearchRecommendationRequest)
  implicit val productHybridRecommendationRequestFormat = jsonFormat1(ProductHybridRecommendationRequest)
  implicit val recommendationFormat = jsonFormat2(Recommendation)
  implicit val hybridRecommendationFormat = jsonFormat3(HybridRecommendation)
}

/**
  * Created by alvsanand on 11/05/16.
  */
object RecommenderController extends SimpleRoutingApp with Logging{
  val ES_HOST_PORT_REGEX = "(.+):(\\d+)".r

  import RecommenderControllerProtocol._

  implicit val system = ActorSystem("ActorSystem")

  def run(serverPort: Int)(implicit mongoConf: MongoConfig, esConf: ESConfig): Unit = {
    implicit val mongoClient = MongoClient(MongoClientURI(mongoConf.uri))
    implicit val esClient = new PreBuiltTransportClient(Settings.EMPTY)
    esConf.transportHosts.split(";")
      .foreach { case ES_HOST_PORT_REGEX(host: String, port: String) => esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt)) }

    logger.info("Launching REST serves[port=%d]".format(serverPort))

    startServer(interface = "localhost", port = serverPort) {
      path("recs" / "cf" / "pro") {
        post(
          entity(as[ProductRecommendationRequest]) { request =>
            complete {
              RecommenderService.getCollaborativeFilteringRecommendations(request).toStream
            }
          }
        )
      } ~
      path("recs" / "cf" / "usr") {
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
      } ~
        path("recs" / "hy" / "pro") {
          post(
            entity(as[ProductHybridRecommendationRequest]) { request =>
              complete {
                RecommenderService.getHybridRecommendations(request).toStream
              }
            }
          )
        }
    }
  }
}
