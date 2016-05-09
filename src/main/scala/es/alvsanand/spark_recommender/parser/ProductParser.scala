package es.alvsanand.spark_recommender.parser

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StructType, _}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class Review(val title: String, author: String, reviewID: String, overall: Option[Double], content: String, date: java.sql.Timestamp)

case class Product(val price: String, features: String, name: String, imgUrl: String, productID: String)

case class ProductReviews(val product: Option[Product], val reviews: Option[List[Review]])

/**
  * Created by alvsanand on 7/05/16.
  */
object ProductParser {
  //  private def reviewStructure(): List[StructField] = {
  //    List(
  //      StructField("Title", StringType),
  //      StructField("Author", StringType),
  //      StructField("ReviewID", StringType),
  //      StructField("Overall", DoubleType),
  //      StructField("Content", StringType),
  //      StructField("Date", StringType)
  //    )
  //  }
  //
  //  private def productStructure(): List[StructField] = {
  //    List(
  //      StructField("Price", StringType),
  //      StructField("Features", StringType),
  //      StructField("Name", StringType),
  //      StructField("ImgURL", StringType),
  //      StructField("ProductID", StringType)
  //    )
  //  }
  //
  //  def productReviewStructure(): StructType = {
  //    StructType(
  //      StructField("Reviews", ArrayType(StructType(reviewStructure)))
  //      :: StructField("ProductInfo", StructType(productStructure))
  //      :: Nil
  //    )
  //  }

  def storeData(dataset: String, mongoHosts: String, mongoDB: String, esHost: String, esIndex: String)(implicit _conf: SparkConf): Unit = {
    val sc = SparkContext.getOrCreate(_conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext._
    import sqlContext.implicits._

    val products = sqlContext.read.json("%s/*/*.json".format(dataset))

    val productReviewsRDD = products.mapPartitions(mapPartitions).cache()

    storeDataInMongo(productReviewsRDD, mongoHosts, mongoDB)(_conf)
    storeDataInES(productReviewsRDD, esHost, esIndex)(_conf)
  }

  private def storeDataInMongo(productReviewsRDD: RDD[ProductReviews], mongoHosts: String, mongoDB: String)(implicit _conf: SparkConf): Unit = {
    val productConfig = MongodbConfigBuilder(Map(Host -> mongoHosts.split(";").toList, Database -> mongoDB, Collection -> "products", SamplingRatio -> 1.0, WriteConcern -> "normal", SplitSize -> 8, SplitKey -> "_id"))
    val reviewsConfig = MongodbConfigBuilder(Map(Host -> mongoHosts.split(";").toList, Database -> mongoDB, Collection -> "reviews", SamplingRatio -> 1.0, WriteConcern -> "normal", SplitSize -> 8, SplitKey -> "_id"))

    val sc = SparkContext.getOrCreate(_conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext._
    import sqlContext.implicits._

    productReviewsRDD.flatMap(x => x.product).toDF().distinct().saveToMongodb(productConfig.build)
    productReviewsRDD.flatMap(x => x.reviews.getOrElse(List[Review]())).distinct().toDF().saveToMongodb(reviewsConfig.build)
  }

  private def storeDataInES(productReviewsRDD: RDD[ProductReviews], esHost: String, esIndex: String)(implicit _conf: SparkConf): Unit = {
    _conf.set("es.nodes", esHost.split(":").head).set("es.port", esHost.split(":").last)
    
    val sc = SparkContext.getOrCreate(_conf)
      
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext._
    import sqlContext.implicits._
    import org.elasticsearch.spark.sql._

    productReviewsRDD.flatMap(x => x.product).toDF().distinct().saveToEs("%s/products".format(esIndex))
  }

  private def mapPartitions(rows: Iterator[Row]): Iterator[ProductReviews] = {
    val df = new SimpleDateFormat("MMMM dd, yyyy", Locale.US)

    rows.map { x =>
      val product = x.fieldIndex("ProductInfo") match {
        case i if i > -1 =>
          val productRow = x(i).asInstanceOf[Row]
          Option(new Product(productRow.getAs[String]("Price"), productRow.getAs[String]("Features"), productRow.getAs[String]("Name"), productRow.getAs[String]("ImgURL"), productRow.getAs[String]("ProductID")))
        case -1 => None
      }

      val reviews = x.fieldIndex("Reviews") match {
        case i if i > -1 =>
          Option(x(i).asInstanceOf[mutable.WrappedArray[Row]].map { x =>
            val date: java.sql.Timestamp = x.getAs[String]("Date") match {
              case s: String => new java.sql.Timestamp(df.parse(s).getTime)
              case null => null
            }
            val overall: Option[Double] = x.getAs[String]("Overall") match {
              case "None" => None
              case s: String => Option(s.toDouble)
            }
            new Review(x.getAs[String]("Title"), x.getAs[String]("Author"), x.getAs[String]("ReviewID"), overall, x.getAs[String]("Content"), date)
          }.toList)
        case -1 => None
      }
      new ProductReviews(product, reviews)
    }
  }
}
