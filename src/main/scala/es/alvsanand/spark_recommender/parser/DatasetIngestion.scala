package es.alvsanand.spark_recommender.parser

import java.net.InetAddress
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Locale

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, WriteConcern => MongodbWriteConcern}
import es.alvsanand.spark_recommender.model
import es.alvsanand.spark_recommender.model._
import es.alvsanand.spark_recommender.utils.{ESConfig, HashUtils, MongoConfig}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import scala.collection.mutable


/**
  * Created by alvsanand on 7/05/16.
  */
object DatasetIngestion {
  val PRODUCTS_COLLECTION_NAME = "products"
  val USERS_COLLECTION_NAME = "users"
  val REVIEWS_COLLECTION_NAME = "reviews"
  val PRODUCTS_INDEX_NAME = "products"
  val ES_HOST_PORT_REGEX = "(.+):(\\d+)".r

  def storeData(dataset: String, datasetFile: Option[String] = None)(implicit _conf: SparkConf, mongoConf: MongoConfig, esConf: ESConfig): Unit = {
    val spark = SparkSession.builder()
      .config(_conf)
      .getOrCreate()

    val jsonFiles = datasetFile match {
      case Some(s) => s"$dataset/$s"
      case None => s"$dataset/*.json"
    }

    import spark.implicits._

    val rawData = spark.read.json(jsonFiles)

    val ds = rawData.as[AmazonProductReviews]
      .mapPartitions(mapAmazonProductReviews)
      .cache()

    val products = ds.map(_.product).toDF()
    val users = ds.flatMap(_.users).distinct().toDF()
    val reviews = ds.flatMap(_.reviews).distinct().toDF()

    storeDataInMongo(products, users, reviews)
    storeDataInES(products)
  }

  private def storeDataInMongo(products: DataFrame, users: DataFrame, reviews: DataFrame)(implicit mongoConf: MongoConfig): Unit = {
    import products.sqlContext.implicits._

    val mongoClient = MongoClient(MongoClientURI(mongoConf.uri))

    mongoClient(mongoConf.db)(PRODUCTS_COLLECTION_NAME).dropCollection()
    mongoClient(mongoConf.db)(USERS_COLLECTION_NAME).dropCollection()
    mongoClient(mongoConf.db)(REVIEWS_COLLECTION_NAME).dropCollection()

    products
      .write
      .option("uri", mongoConf.uri)
      .option("collection", PRODUCTS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    users
      .write.option("uri", mongoConf.uri)
      .option("collection", USERS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    reviews
      .write.option("uri", mongoConf.uri)
      .option("collection", REVIEWS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    mongoClient(mongoConf.db)(PRODUCTS_COLLECTION_NAME).createIndex(MongoDBObject("id" -> 1))
    mongoClient(mongoConf.db)(PRODUCTS_COLLECTION_NAME).createIndex(MongoDBObject("extId" -> 1))
    mongoClient(mongoConf.db)(USERS_COLLECTION_NAME).createIndex(MongoDBObject("id" -> 1))
    mongoClient(mongoConf.db)(USERS_COLLECTION_NAME).createIndex(MongoDBObject("extId" -> 1))
    mongoClient(mongoConf.db)(REVIEWS_COLLECTION_NAME).createIndex(MongoDBObject("userId" -> 1))
    mongoClient(mongoConf.db)(REVIEWS_COLLECTION_NAME).createIndex(MongoDBObject("productId" -> 1))
  }

  private def storeDataInES(products: DataFrame)(implicit esConf: ESConfig): Unit = {
    import products.sqlContext.implicits._

    val options = Map("es.nodes" -> esConf.httpHosts,
      "es.http.timeout" -> "100m",
      "es.mapping.id" -> "id")
    val indexName = esConf.index
    val typeName = s"$indexName/$PRODUCTS_INDEX_NAME"

    val esClient = new PreBuiltTransportClient(Settings.EMPTY)
    esConf.transportHosts.split(";")
      .foreach { case ES_HOST_PORT_REGEX(host: String, port: String) => esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt)) }

    if (esClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists) {
      esClient.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet()
    }
    esClient.admin().indices().create(new CreateIndexRequest(indexName)).actionGet()

    products.toDF()
      .write.options(options)
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(typeName)
  }

  private def mapAmazonProductReviews(iterator: Iterator[AmazonProductReviews]): Iterator[ProductReview] = {
    val df = new SimpleDateFormat("MMMM dd, yyyy", Locale.US)

    iterator.flatMap { amazonProductReviews =>
      if (amazonProductReviews.ProductInfo == null ||
        amazonProductReviews.ProductInfo.ProductID == null) {
        List.empty[ProductReview]
      }
      else {
        val product = new model.Product(
          HashUtils.hash(amazonProductReviews.ProductInfo.ProductID),
          amazonProductReviews.ProductInfo.ProductID,
          amazonProductReviews.ProductInfo.Name,
          amazonProductReviews.ProductInfo.Price,
          amazonProductReviews.ProductInfo.Features,
          amazonProductReviews.ProductInfo.ImgURL)

        if (amazonProductReviews.Reviews != null) {
          val usersAndReviews = amazonProductReviews.Reviews.flatMap { amazonReview =>
            try {
              val date: Timestamp = amazonReview.Date match {
                case s: String => new Timestamp(df.parse(s).getTime)
                case null => null
              }

              val overall: Option[Double] = amazonReview.Overall match {
                case "None" => None
                case s: String => Option(s.toDouble)
              }

              val userId = HashUtils.hash(amazonReview.Author)
              val extUserId = amazonReview.Author

              Option((
                User(userId, extUserId),
                Review(amazonReview.ReviewID,
                  userId,
                  product.id,
                  amazonReview.Title,
                  overall,
                  amazonReview.Content,
                  date)
              ))
            }
            catch {
              case e: Exception => Option.empty[(User, Review)]
            }
          }

          Option(ProductReview(product, usersAndReviews.map(_._1).distinct.toArray, usersAndReviews.map(_._2).toArray))
        }
        else {
          Option(ProductReview(product))
        }
      }
    }
  }
}
