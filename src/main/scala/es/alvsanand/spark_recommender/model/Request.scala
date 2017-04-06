package es.alvsanand.spark_recommender.model

case class Recommendation(productId: Int, rating: Double)
case class HybridRecommendation(productId: Int, originalRating: Double, hybridRating: Double)

case class ProductRecommendationRequest(productId: Int)

case class ProductHybridRecommendationRequest(productId: Int)

case class UserRecommendationRequest(userId: Int)

case class SearchRecommendationRequest(text: String)