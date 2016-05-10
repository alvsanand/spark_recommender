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
      StructField("pid", StringType),
      StructField("r", DoubleType)
    )
  }

  def userRecsStructure(): List[StructField] = {
    List(
      StructField("userId", StringType),
      StructField("recs", ArrayType(StructType(productRecommendationStructure)))
    )
  }

  def productRecsStructure(): List[StructField] = {
    List(
      StructField("productId", StringType),
      StructField("recs", ArrayType(StructType(productRecommendationStructure)))
    )
  }

  def calculateRecs(maxRecs: Int)(implicit _conf: SparkConf, mongoConf: MongoConfig): Unit = {
    val reviewsConfig = MongodbConfigBuilder(Map(Host -> mongoConf.hosts.split(";").toList, Database -> mongoConf.db, Collection -> "reviews"))

    val sc = SparkContext.getOrCreate(_conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val mongoRDD = sqlContext.fromMongoDB(reviewsConfig.build(), Option(StructType(reviewStructure)))
    mongoRDD.registerTempTable("reviews")

    val ratingsDF = sqlContext.sql("select userId, productId, overall from reviews where userId is not null and productId is not null and overall is not null").cache

    val ratingsRDD = ratingsDF.map { row => Rating(row.getAs[String]("userId").hashCode, row.getAs[String]("productId").hashCode, row.getAs[Double]("overall").toFloat) }
    val usersRDD: RDD[(Int, String)] = ratingsDF.map( row => row.getAs[String]("userId") ).map( userId => (userId.hashCode, userId) )
    val productsRDD: RDD[(Int, String)] = ratingsDF.map( row => row.getAs[String]("productId") ).map( productId => (productId.hashCode, productId) )
    val productsMap: Broadcast[Map[Int, String]] = sc.broadcast(productsRDD.collect().toMap)

    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratingsRDD, rank, numIterations, 0.01)

    calculateUserRecs(maxRecs, model, usersRDD, productsMap)
    calculateProductRecs(maxRecs, model, productsMap.value)
  }

  private def calculateUserRecs(maxRecs: Int, model: MatrixFactorizationModel, usersRDD: RDD[(Int, String)], productsMap: Broadcast[Map[Int, String]])(implicit _conf: SparkConf, mongoConf: MongoConfig): Unit = {
    val sc = SparkContext.getOrCreate(_conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val userRecs: RDD[Row] = model.recommendProductsForUsers(maxRecs)
      .join(usersRDD)
      .map { case (userIdInt, (ratings, userId))  => Row(userId, ratings.map { rating => Row(productsMap.value.get(rating.product).get, rating.rating) }.toList) }

    val userRecsConfig = MongodbConfigBuilder(Map(Host -> mongoConf.hosts.split(";").toList, Database -> mongoConf.db, Collection -> "userRecs"))

    sqlContext.createDataFrame(userRecs, StructType(userRecsStructure))
      .saveToMongodb(userRecsConfig.build)
  }

  private def calculateProductRecs(maxRecs: Int, model: MatrixFactorizationModel, productsMap: Map[Int, String])(implicit _conf: SparkConf, mongoConf: MongoConfig): Unit = {
    val sc = SparkContext.getOrCreate(_conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val productRecs: RDD[Row] = sc.parallelize(productsMap.toList.map { case (idInt, id) =>
      val itemFactor = model.productFeatures.lookup(idInt).head
      val itemVector = new DoubleMatrix(itemFactor)

      val sims: RDD[(String, Double)]  = model.productFeatures.map { case (currentIdInt, factor) =>
        val currentId = productsMap(currentIdInt)

        val factorVector = new DoubleMatrix(factor)
        val sim = cosineSimilarity(factorVector, itemVector)

        (currentId, sim)
      }

      val recs = sims.filter{ case (currentId, sim) => currentId != id }
                     .top(maxRecs)(Ordering.by[(String, Double), Double] { case (currentId, sim) => sim })
                     .map { case (currentId, sim) => Row(currentId, sim) }.toList

      Row(id, recs)
    })

    val productRecsConfig = MongodbConfigBuilder(Map(Host -> mongoConf.hosts.split(";").toList, Database -> mongoConf.db, Collection -> "productRecs"))

    sqlContext.createDataFrame(productRecs, StructType(productRecsStructure))
              .saveToMongodb(productRecsConfig.build)
  }

  private def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }
}
