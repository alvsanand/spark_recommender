package es.alvsanand.spark_recommender.parser

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StructType, _}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class Review(reviewId: String, userId: String, productId: String, val title: String, overall: Option[Double], content: String, date: java.sql.Timestamp)

case class Product(productId: String, name: String, val price: String, features: String, imgUrl: String)

/**
  * Created by alvsanand on 7/05/16.
  */
object ProductParser {

  def storeData(dataset: String, mongoHosts: String, mongoDB: String, esHost: String, esIndex: String)(implicit _conf: SparkConf): Unit = {
    val sc = SparkContext.getOrCreate(_conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext._
    import sqlContext.implicits._

    val products = sqlContext.read.json("%s/*/*.json".format(dataset))

    val productReviewsRDD = products.mapPartitions(mapPartitions).cache()

    storeDataInMongo(productReviewsRDD, mongoHosts, mongoDB)(_conf)
    storeDataInES(productReviewsRDD.map(x => x._1), esHost, esIndex)(_conf)
  }

  private def storeDataInMongo(productReviewsRDD: RDD[(Product, Option[List[Review]])], mongoHosts: String, mongoDB: String)(implicit _conf: SparkConf): Unit = {
    val productConfig = MongodbConfigBuilder(Map(Host -> mongoHosts.split(";").toList, Database -> mongoDB, Collection -> "products", SamplingRatio -> 1.0, WriteConcern -> "normal", SplitSize -> 8, SplitKey -> "_id"))
    val reviewsConfig = MongodbConfigBuilder(Map(Host -> mongoHosts.split(";").toList, Database -> mongoDB, Collection -> "reviews", SamplingRatio -> 1.0, WriteConcern -> "normal", SplitSize -> 8, SplitKey -> "_id"))

    val sc = SparkContext.getOrCreate(_conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext._
    import sqlContext.implicits._

    productReviewsRDD.map(x => x._1).toDF().distinct().saveToMongodb(productConfig.build)
    productReviewsRDD.flatMap(x => x._2.getOrElse(List[Review]())).toDF().distinct().saveToMongodb(reviewsConfig.build)
  }

  private def storeDataInES(productReviewsRDD: RDD[Product], esHost: String, esIndex: String)(implicit _conf: SparkConf): Unit = {
    val options = Map("es.nodes" -> esHost, "es.http.timeout" -> "100m")
    
    val sc = SparkContext.getOrCreate(_conf)
      
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext._
    import sqlContext.implicits._
    import org.elasticsearch.spark.sql._

    productReviewsRDD.toDF().distinct().saveToEs("%s/products".format(esIndex), options)
  }

  private def mapPartitions(rows: Iterator[Row]): Iterator[(Product, Option[List[Review]])] = {
    val df = new SimpleDateFormat("MMMM dd, yyyy", Locale.US)

    rows.flatMap { x =>
      if(x.fieldIndex("ProductInfo") == -1 || x.getAs[Row]("ProductInfo").fieldIndex("ProductID") == -1 || x.getAs[Row]("ProductInfo").getAs[String]("ProductID")==null){
        None
      }
      else{
        val productRow = x.getAs[Row]("ProductInfo")
        val product = new Product(productRow.getAs[String]("ProductID"), productRow.getAs[String]("Name"), productRow.getAs[String]("Price"), productRow.getAs[String]("Features"), productRow.getAs[String]("ImgURL"))

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
              new Review(x.getAs[String]("ReviewID"), x.getAs[String]("Author"), product.productId, x.getAs[String]("Title"), overall, x.getAs[String]("Content"), date)
            }.toList)
          case -1 => None
        }

        Option((product, reviews))
      }
    }
  }
}
