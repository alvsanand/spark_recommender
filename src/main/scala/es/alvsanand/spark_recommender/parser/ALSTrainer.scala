package es.alvsanand.spark_recommender.parser

import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config._
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

/**
  * Created by alvsanand on 7/05/16.
  */
object ALSTrainer {
  def reviewStructure(): List[StructField] = {
    List(
      StructField("reviewId", StringType),
      StructField("userId", StringType),
      StructField("productId", StringType),
      StructField("title", StringType),
      StructField("overall", DoubleType),
      StructField("content", StringType),
      StructField("date", StringType)
    )
  }
  def productRecommendationStructure(): List[StructField] = {
    List(
      StructField("id", StringType),
      StructField("r",  DoubleType)
    )
  }

  def userRecommendationsStructure(): List[StructField] = {
    List(
      StructField("userId", StringType),
      StructField("recs", StructType(productRecommendationStructure))
    )
  }

  def productRecommendationsStructure(): List[StructField] = {
    List(
      StructField("productId", StringType),
      StructField("recs", StructType(productRecommendationStructure))
    )
  }

  def calculateRecommendations(mongoHosts: String, mongoDB: String, maxRecommendations: Int)(implicit _conf: SparkConf): Unit = {
    val reviewsConfig = MongodbConfigBuilder(Map(Host -> mongoHosts.split(";").toList, Database -> mongoDB, Collection -> "reviews", SamplingRatio -> 1.0, WriteConcern -> "normal", SplitSize -> 8, SplitKey -> "_id"))
    val userRecommendationsConfig = MongodbConfigBuilder(Map(Host -> mongoHosts.split(";").toList, Database -> mongoDB, Collection -> "userRecommendations", SamplingRatio -> 1.0, WriteConcern -> "normal", SplitSize -> 8, SplitKey -> "_id"))
    val productRecommendationsConfig = MongodbConfigBuilder(Map(Host -> mongoHosts.split(";").toList, Database -> mongoDB, Collection -> "productRecommendations", SamplingRatio -> 1.0, WriteConcern -> "normal", SplitSize -> 8, SplitKey -> "_id"))

    val sc = SparkContext.getOrCreate(_conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext._
    import sqlContext.implicits._

    val mongoRDD = sqlContext.fromMongoDB(reviewsConfig.build(), Option(StructType(reviewStructure)))
    mongoRDD.registerTempTable("reviews")

    val ratings = sqlContext.sql("select userId, productId, overall from reviews").map(x => Rating(x.getAs[String]("reviewId").hashCode, x.getAs[String]("userId").hashCode, x.getAs[Double]("overall").toFloat))

    val usersRDD = sqlContext.sql("select distinct userId from reviews").map(x => x.getAs[String]("reviewId")).map(x => (x.hashCode, x))
    val productsRDD = sqlContext.sql("select distinct productId from reviews").map(x => x.getAs[String]("productId")).map(x => (x.hashCode, x))
    val products = sc.broadcast(productsRDD.collect().toMap)

    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    val userRecommendations: RDD[Row] = model.recommendProductsForUsers(maxRecommendations)
      .join(usersRDD)
      .map { x => Row(x._2._2, x._2._1.map { x => Row(products.value.get(x.product), x.rating) }.toList) }

    sqlContext.createDataFrame(userRecommendations, StructType(userRecommendationsStructure))
              .saveToMongodb(userRecommendationsConfig.build)

    val productRecommendations: RDD[Row] = productsRDD.map { x =>
      val itemFactor = model.productFeatures.lookup(x._1).head
      val itemVector = new DoubleMatrix(itemFactor)
      cosineSimilarity(itemVector, itemVector)
      val sims = model.productFeatures.map{ case (id, factor) =>
        val factorVector = new DoubleMatrix(factor)
        val sim = cosineSimilarity(factorVector, itemVector)
        (x._2, sim)
      }

      val recs = sims.top(maxRecommendations)(Ordering.by[(String, Double), Double] { case (id, similarity) => similarity })

      Row(x._2, recs.map( x => Row(x._1, x._2)).toList)
    }

    sqlContext.createDataFrame(productRecommendations, StructType(productRecommendationsStructure))
      .saveToMongodb(userRecommendationsConfig.build)
  }

  private def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }

//  private def
}
