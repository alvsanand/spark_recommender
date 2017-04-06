package es.alvsanand.spark_recommender.trainer

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, WriteConcern => MongodbWriteConcern}
import es.alvsanand.spark_recommender.model.{ProductRecommendation, Rec, Review, UserRecommendation}
import es.alvsanand.spark_recommender.parser.DatasetIngestion
import es.alvsanand.spark_recommender.utils.MongoConfig
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.jblas.DoubleMatrix

import scala.collection.mutable

/**
  * Created by alvsanand on 7/05/16.
  */
object ALSTrainer {
  val USER_RECS_COLLECTION_NAME = "userRecs"
  val PRODUCT_RECS_COLLECTION_NAME = "productRecs"

  val MAX_RATING = 5.0F
  val MAX_RECOMMENDATIONS = 100

  def calculateRecs(maxRecs: Int)(implicit _conf: SparkConf, mongoConf: MongoConfig): Unit = {
    val spark = SparkSession.builder()
      .config(_conf)
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val ratings = spark.read
      .option("uri", mongoConf.uri)
      .option("collection", DatasetIngestion.REVIEWS_COLLECTION_NAME)
      .format("com.mongodb.spark.sql")
      .load()
      .select($"userId".as("user"), $"productId".as("item"), $"overall".cast(FloatType).as("rating"))
      .where($"userId".isNotNull && $"productId".isNotNull && $"overall".isNotNull)
      .cache

    val users = spark.read
      .option("uri", mongoConf.uri)
      .option("collection", DatasetIngestion.USERS_COLLECTION_NAME)
      .format("com.mongodb.spark.sql")
      .load()
      .select($"id")
      .distinct
      .map(r => r.getAs[Int]("id"))
      .cache

    val products = spark.read
      .option("uri", mongoConf.uri)
      .option("collection", DatasetIngestion.PRODUCTS_COLLECTION_NAME)
      .format("com.mongodb.spark.sql")
      .load()
      .select($"id")
      .distinct
      .map(r => r.getAs[Int]("id"))
      .cache

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("user")
      .setItemCol("item")
      .setRatingCol("rating")
    val model = als.fit(ratings)

    implicit val mongoClient = MongoClient(MongoClientURI(mongoConf.uri))

    calculateUserRecs(maxRecs, model, users, products)
    calculateProductRecs(maxRecs, model, products)
  }

  private def calculateUserRecs(maxRecs: Int, model: ALSModel, users: Dataset[Int], products: Dataset[Int])(implicit mongoConf: MongoConfig, mongoClient: MongoClient): Unit = {
    import users.sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val userProductsJoin = users.crossJoin(products)
    val userRating = userProductsJoin.map { row => Rating(row.getAs[Int](0), row.getAs[Int](1), 0) }

    object RatingOrder extends Ordering[Row] {
      def compare(x: Row, y: Row) = y.getAs[Float](model.getPredictionCol) compare x.getAs[Float](model.getPredictionCol)
    }

    val recommendations = model.transform(userRating)
      .filter(col(model.getPredictionCol) > 0 && !col(model.getPredictionCol).isNaN )
      .groupByKey(p => (p.getAs[Int](model.getUserCol)))
      .mapGroups { case (userId, predictions) =>
        val recommendations = predictions.toSeq.sorted(RatingOrder)
          .take(MAX_RECOMMENDATIONS)
          .map(p => Rec(p.getAs[Int](model.getItemCol), p.getAs[Float](model.getPredictionCol).toDouble))

        UserRecommendation(userId, recommendations)
      }


    mongoClient(mongoConf.db)(USER_RECS_COLLECTION_NAME).dropCollection()

    recommendations
      .write
      .option("uri", mongoConf.uri)
      .option("collection", USER_RECS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save

    mongoClient(mongoConf.db)(USER_RECS_COLLECTION_NAME).createIndex(MongoDBObject("userId" -> 1))
  }

  private def calculateProductRecs(maxRecs: Int, model: ALSModel, products: Dataset[Int])(implicit mongoConf: MongoConfig, mongoClient: MongoClient): Unit = {
    import products.sqlContext.implicits._
    import org.apache.spark.sql.functions._

    object RatingOrder extends Ordering[(Int, Int, Double)] {
      def compare(x: (Int, Int, Double), y: (Int, Int, Double)) = y._3 compare x._3
    }

    val recommendations = model.itemFactors.crossJoin(model.itemFactors)
      .filter(r => r.getAs[Int](0) != r.getAs[Int](2))
      .map { r =>
        val idA = r.getAs[Int](0)
        val idB = r.getAs[Int](2)
        val featuresA = r.getAs[mutable.WrappedArray[Float]](1).map(_.toDouble).toArray
        val featuresB = r.getAs[mutable.WrappedArray[Float]](3).map(_.toDouble).toArray

        (idA, idB, cosineSimilarity(new DoubleMatrix(featuresA), new DoubleMatrix(featuresB)))
      }
      .filter(col("_3") > 0 && !col("_3").isNaN)
      .groupByKey(p => p._1)
      .mapGroups { case (productId, predictions) =>
        val recommendations = predictions.toSeq.sorted(RatingOrder)
          .take(MAX_RECOMMENDATIONS)
          .map(p => Rec(p._2, p._3.toDouble))

        ProductRecommendation(productId, recommendations)
      }
      .toDF

    mongoClient(mongoConf.db)(PRODUCT_RECS_COLLECTION_NAME).dropCollection()

    recommendations.write
      .option("uri", mongoConf.uri)
      .option("collection", PRODUCT_RECS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save

    mongoClient(mongoConf.db)(PRODUCT_RECS_COLLECTION_NAME).createIndex(MongoDBObject("productid" -> 1))
  }

  private def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }
}
