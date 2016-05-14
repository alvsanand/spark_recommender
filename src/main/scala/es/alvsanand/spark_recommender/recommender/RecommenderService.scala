package es.alvsanand.spark_recommender.recommender

import com.mongodb.casbah.Imports._
import es.alvsanand.spark_recommender.model.{ProductRecommendationRequest, Recommendation, SearchRecommendationRequest, UserRecommendationRequest}
import es.alvsanand.spark_recommender.parser.DatasetIngestion
import es.alvsanand.spark_recommender.trainer.ALSTrainer
import es.alvsanand.spark_recommender.utils.{ESConfig, MongoConfig}
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.index.query.{MoreLikeThisQueryBuilder, QueryBuilders}
import org.elasticsearch.search.SearchHits

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

  private def parseESResponse(response: SearchResponse): List[Recommendation] = {
    response.getHits match {
      case null => List[Recommendation]()
      case hits: SearchHits if hits.getTotalHits == 0 => List[Recommendation]()
      case hits: SearchHits if hits.getTotalHits > 0 => hits.getHits.map { hit => new Recommendation(hit.getId, hit.getScore) }.toList
    }
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

  def getContentBasedMoreLikeThisRecommendations(request: ProductRecommendationRequest)(implicit esClient: Client, esConf: ESConfig): List[Recommendation] = {
    val indexName = esConf.index

    val query = QueryBuilders.moreLikeThisQuery("name", "features")
      .addLikeItem(new MoreLikeThisQueryBuilder.Item(indexName, DatasetIngestion.PRODUCTS_INDEX_NAME, request.productId))

    return parseESResponse(esClient.prepareSearch().setQuery(query).setSize(MAX_RECOMMENDATIONS).execute().actionGet())
  }

  def getContentBasedSearchRecommendations(request: SearchRecommendationRequest)(implicit esClient: Client, esConf: ESConfig): List[Recommendation] = {
    val indexName = esConf.index

    val query = QueryBuilders.multiMatchQuery(request.text, "name", "features")

    return parseESResponse(esClient.prepareSearch().setIndices(indexName).setTypes(DatasetIngestion.PRODUCTS_INDEX_NAME).setQuery(query).setSize(MAX_RECOMMENDATIONS).execute().actionGet())
  }
}
