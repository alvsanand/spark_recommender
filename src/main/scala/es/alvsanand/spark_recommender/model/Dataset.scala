package es.alvsanand.spark_recommender.model

import java.sql.Timestamp


case class AmazonReview(Author: String, Content: String, Date: String, Overall: String, ReviewID: String, Title: String)

case class AmazonProductInfo(Features: String, ImgURL: String, Name: String, Price: String, ProductID: String)

case class AmazonProductReviews(ProductInfo: AmazonProductInfo, Reviews: Seq[AmazonReview])

/**
  * Created by alvsanand on 11/05/16.
  */
case class Product(id: Int, extId: String, name: String, price: String, features: String, imgUrl: String)

/*
  * Created by alvsanand on 31/03/17.
 */
case class User(id: Int, extId: String)

/**
  * Created by alvsanand on 11/05/16.
  */
case class Review(reviewId: String, userId: Int, productId: Int, val title: String, overall: Option[Double], content: String, date: Timestamp)


case class ProductReview(product: Product, users: Array[User] = Array.empty, reviews: Array[Review] = Array.empty)
