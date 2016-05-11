package es.alvsanand.spark_recommender.model

/**
  * Created by asantos on 11/05/16.
  */
case class Recommendation(productId: String, rating: Double)

case class ProductRecommendationRequest(productId: String)
case class UserRecommendationRequest(userId: String)