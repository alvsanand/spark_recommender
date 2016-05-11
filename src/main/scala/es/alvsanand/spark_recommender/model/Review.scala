package es.alvsanand.spark_recommender.model

/**
  * Created by asantos on 11/05/16.
  */
case class Review(reviewId: String, userId: String, productId: String, val title: String, overall: Option[Double], content: String, date: java.sql.Timestamp)
