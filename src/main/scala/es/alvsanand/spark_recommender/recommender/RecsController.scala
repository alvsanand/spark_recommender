package es.alvsanand.spark_recommender.recommender

import com.mongodb.casbah.MongoConnection
import es.alvsanand.spark_recommender.model.{ProductRecommendationRequest, Recommendation, UserRecommendationRequest}
import spray.json.{DefaultJsonProtocol, NullOptions}

/**
  * Created by asantos on 11/05/16.
  */
object RecsService {

  def getCollaborativeFilteringRecommendations(request: ProductRecommendationRequest): List[Recommendation] = {
    return List[Recommendation]()
  }

  def getCollaborativeFilteringRecommendations(request: UserRecommendationRequest): List[Recommendation] = {
    return List[Recommendation]()
  }
}

object RecsControllerProtocol extends DefaultJsonProtocol with NullOptions {
  implicit val songFormat = jsonFormat1(ProductRecommendationRequest)
  implicit val personFormat = jsonFormat1(UserRecommendationRequest)
  implicit val groupFormat = jsonFormat2(Recommendation)
}
