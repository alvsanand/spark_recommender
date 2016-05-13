package es.alvsanand.spark_recommender.recommender

import com.mongodb.casbah.Imports._
import es.alvsanand.spark_recommender.model.{ProductRecommendationRequest, Recommendation, UserRecommendationRequest}
import es.alvsanand.spark_recommender.trainer.ALSTrainer
import es.alvsanand.spark_recommender.utils.MongoConfig

/**
  * Created by asantos on 11/05/16.
  */
object RecommenderService {
  private val MAX_RECOMMENDATIONS = 10

  private def parseUserRecs(o: DBObject): List[Recommendation] = {
    o.getAs[MongoDBList]("recs").getOrElse(MongoDBList()).map { case (o: DBObject) => parseRec(o) }.toList.sortBy(x => x.rating).reverse.take(MAX_RECOMMENDATIONS)
  }

  private def parseProductRecs(o: DBObject): List[Recommendation] = {
    o.getAs[MongoDBList]("recs").getOrElse(MongoDBList()).map { case (o: DBObject) => parseRec(o) }.toList.sortBy(x => x.rating).reverse.take(MAX_RECOMMENDATIONS)
  }

  private def parseRec(o: DBObject): Recommendation = {
    new Recommendation(o.getAs[String]("pid").getOrElse(null), o.getAs[Double]("r").getOrElse(0))
  }

  def getCollaborativeFilteringRecommendations(request: ProductRecommendationRequest)(implicit mongoClient: MongoClient, mongoConf: MongoConfig): List[Recommendation] = {
    return parseProductRecs(mongoClient(mongoConf.db)(ALSTrainer.PRODUCT_RECS_COLLECTION_NAME).findOne(MongoDBObject("productId" -> request.productId)).getOrElse(MongoDBObject()))
  }

  def getCollaborativeFilteringRecommendations(request: UserRecommendationRequest)(implicit mongoClient: MongoClient, mongoConf: MongoConfig): List[Recommendation] = {
    return parseUserRecs(mongoClient(mongoConf.db)(ALSTrainer.USER_RECS_COLLECTION_NAME).findOne(MongoDBObject("userId" -> request.userId)).getOrElse(MongoDBObject()))
  }


  //  def getContentBasedRecommendations(request: ProductRecommendationRequest)(implicit esClient: Client, esConf: ESConfig): List[Recommendation] = {
  //    val builder = DBObject.newBuilder
  //    builder += "userId" -> request.userId
  //
  //    return parseUserRecs(mongoClient(mongoConf.db)(ALSTrainer.USER_RECS_COLLECTION_NAME).findOne(builder.result()).getOrElse(MongoDBObject()))
  //  }
}
