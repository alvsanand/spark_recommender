package es.alvsanand.spark_recommender.parser

import java.text.SimpleDateFormat
import java.util.Locale

import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config._
import es.alvsanand.spark_recommender.utils.{ESConfig, MongoConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

case class Review(reviewId: String, userId: String, productId: String, val title: String, overall: Option[Double], content: String, date: java.sql.Timestamp)

case class Product(productId: String, name: String, val price: String, features: String, imgUrl: String)

/**
  * Created by alvsanand on 7/05/16.
  */
object ProductParser {

  def storeData(dataset: String)(implicit _conf: SparkConf, mongoConf: MongoConfig, esConf: ESConfig): Unit = {
    val sc = SparkContext.getOrCreate(_conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val products = sqlContext.read.json("%s/*/*.json".format(dataset))

    val productReviewsRDD = products.mapPartitions(mapPartitions).cache()

    storeDataInMongo(productReviewsRDD)
    storeDataInES(productReviewsRDD.map { case ( product, reviews ) => product } )
  }

  private def storeDataInMongo(productReviewsRDD: RDD[(Product, Option[List[Review]])])(implicit _conf: SparkConf, mongoConf: MongoConfig): Unit = {
    val productConfig = MongodbConfigBuilder(Map(Host -> mongoConf.hosts.split(";").toList, Database -> mongoConf.db, Collection -> "products"))
    val reviewsConfig = MongodbConfigBuilder(Map(Host -> mongoConf.hosts.split(";").toList, Database -> mongoConf.db, Collection -> "reviews"))

    val sc = SparkContext.getOrCreate(_conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    productReviewsRDD.map { case ( product, reviews ) => product } .toDF().distinct().saveToMongodb(productConfig.build)
    productReviewsRDD.flatMap { case ( product, reviews ) => reviews.getOrElse(List[Review]()) }.toDF().distinct().saveToMongodb(reviewsConfig.build)
  }

  private def storeDataInES(productReviewsRDD: RDD[Product])(implicit _conf: SparkConf, esConf: ESConfig): Unit = {
    val options = Map("es.nodes" -> esConf.hosts, "es.http.timeout" -> "100m")
    
    val sc = SparkContext.getOrCreate(_conf)
      
    val sqlContext = SQLContext.getOrCreate(sc)
    import org.elasticsearch.spark.sql._
    import sqlContext.implicits._

    productReviewsRDD.toDF().distinct().saveToEs("%s/products".format(esConf.index), options)
  }

  private def mapPartitions(rows: Iterator[Row]): Iterator[(Product, Option[List[Review]])] = {
    val df = new SimpleDateFormat("MMMM dd, yyyy", Locale.US)

    rows.flatMap { row =>
      if(row.fieldIndex("ProductInfo") == -1 || row.getAs[Row]("ProductInfo").fieldIndex("ProductID") == -1 || row.getAs[Row]("ProductInfo").getAs[String]("ProductID")==null){
        None
      }
      else{
        val productRow = row.getAs[Row]("ProductInfo")
        val product = new Product(productRow.getAs[String]("ProductID"), productRow.getAs[String]("Name"), productRow.getAs[String]("Price"), productRow.getAs[String]("Features"), productRow.getAs[String]("ImgURL"))

        val reviews = row.fieldIndex("Reviews") match {
          case i if i > -1 =>
            Option(row(i).asInstanceOf[mutable.WrappedArray[Row]].map { reviewRow =>
              val date: java.sql.Timestamp = reviewRow.getAs[String]("Date") match {
                case s: String => new java.sql.Timestamp(df.parse(s).getTime)
                case null => null
              }
              val overall: Option[Double] = reviewRow.getAs[String]("Overall") match {
                case "None" => None
                case s: String => Option(s.toDouble)
              }
              new Review(reviewRow.getAs[String]("ReviewID"), reviewRow.getAs[String]("Author"), product.productId, reviewRow.getAs[String]("Title"), overall, reviewRow.getAs[String]("Content"), date)
            }.toList)
          case -1 => None
        }

        Option((product, reviews))
      }
    }
  }
}
