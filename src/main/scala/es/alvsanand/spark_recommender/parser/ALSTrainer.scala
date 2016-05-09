package es.alvsanand.spark_recommender.parser

import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config._
import es.alvsanand.spark_recommender.utils.MongoConfig
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
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
      StructField("_id", StringType),
      StructField("reviewId", StringType),
      StructField("userId", StringType),
      StructField("productId", StringType),
      StructField("title", StringType),
      StructField("overall", DoubleType),
      StructField("content", StringType),
      StructField("date", DateType)
    )
  }

  def productRecommendationStructure(): List[StructField] = {
    List(
      StructField("id", StringType),
      StructField("r", DoubleType)
    )
  }

  def userRecsStructure(): List[StructField] = {
    List(
      StructField("userId", StringType),
      StructField("recs", StructType(productRecommendationStructure))
    )
  }

  def productRecsStructure(): List[StructField] = {
    List(
      StructField("productId", StringType),
      StructField("recs", StructType(productRecommendationStructure))
    )
  }

  def calculateRecs(maxRecs: Int)(implicit _conf: SparkConf, mongoConf: MongoConfig): Unit = {
    val reviewsConfig = MongodbConfigBuilder(Map(Host -> mongoConf.hosts.split(";").toList, Database -> mongoConf.db, Collection -> "reviews"))

    val sc = SparkContext.getOrCreate(_conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val mongoRDD = sqlContext.fromMongoDB(reviewsConfig.build(), Option(StructType(reviewStructure)))
    mongoRDD.registerTempTable("reviews")

    val ratingsDF = sqlContext.sql("select userId, productId, overall from reviews where userId is not null and productId is not null and overall is not null").cache

    val ratingsRDD = ratingsDF.map(x => Rating(x.getAs[String]("userId").hashCode, x.getAs[String]("productId").hashCode, x.getAs[Double]("overall").toFloat))
    val usersRDD: RDD[(Int, String)] = ratingsDF.map(x => x.getAs[String]("userId")).map(x => (x.hashCode, x))
    val productsRDD: RDD[(Int, String)] = ratingsDF.map(x => x.getAs[String]("productId")).map(x => (x.hashCode, x))
    val productsMap: Broadcast[Map[Int, String]] = sc.broadcast(productsRDD.collect().toMap)

    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratingsRDD, rank, numIterations, 0.01)

    calculateUserRecs(maxRecs, model, usersRDD, productsMap)
    calculateProductRecs(maxRecs, model, productsRDD)
  }

  private def calculateUserRecs(maxRecs: Int, model: MatrixFactorizationModel, usersRDD: RDD[(Int, String)], productsMap: Broadcast[Map[Int, String]])(implicit _conf: SparkConf, mongoConf: MongoConfig): Unit = {
    val sc = SparkContext.getOrCreate(_conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val userRecs: RDD[Row] = model.recommendProductsForUsers(maxRecs)
      .join(usersRDD)
      .map { x => Row(x._2._2, x._2._1.map { x => Row(productsMap.value.get(x.product), x.rating) }.toList) }

    val userRecsConfig = MongodbConfigBuilder(Map(Host -> mongoConf.hosts.split(";").toList, Database -> mongoConf.db, Collection -> "userRecs"))

    sqlContext.createDataFrame(userRecs, StructType(userRecsStructure))
      .saveToMongodb(userRecsConfig.build)
  }

  private def calculateProductRecs(maxRecs: Int, model: MatrixFactorizationModel, productsRDD: RDD[(Int, String)])(implicit _conf: SparkConf, mongoConf: MongoConfig): Unit = {
    val sc = SparkContext.getOrCreate(_conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val productRecs: RDD[Row] = productsRDD.map { x =>
      val itemFactor = model.productFeatures.lookup(x._1).head
      val itemVector = new DoubleMatrix(itemFactor)
      cosineSimilarity(itemVector, itemVector)
      val sims = model.productFeatures.map { case (id, factor) =>
        val factorVector = new DoubleMatrix(factor)
        val sim = cosineSimilarity(factorVector, itemVector)
        (x._2, sim)
      }

      val recs = sims.top(maxRecs)(Ordering.by[(String, Double), Double] { case (id, similarity) => similarity })

      Row(x._2, recs.map(x => Row(x._1, x._2)).toList)
    }

    val productRecsConfig = MongodbConfigBuilder(Map(Host -> mongoConf.hosts.split(";").toList, Database -> mongoConf.db, Collection -> "productRecs"))

    sqlContext.createDataFrame(productRecs, StructType(productRecsStructure))
      .saveToMongodb(productRecsConfig.build)
  }

  private def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }

  //  private def
}
