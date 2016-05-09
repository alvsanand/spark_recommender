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
    storeDataInES(productReviewsRDD.map(x => x._1))
  }

  private def storeDataInMongo(productReviewsRDD: RDD[(Product, Option[List[Review]])])(implicit _conf: SparkConf, mongoConf: MongoConfig): Unit = {
    val productConfig = MongodbConfigBuilder(Map(Host -> mongoConf.hosts.split(";").toList, Database -> mongoConf.db, Collection -> "products"))
    val reviewsConfig = MongodbConfigBuilder(Map(Host -> mongoConf.hosts.split(";").toList, Database -> mongoConf.db, Collection -> "reviews"))

    val sc = SparkContext.getOrCreate(_conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    productReviewsRDD.map(x => x._1).toDF().distinct().saveToMongodb(productConfig.build)
    productReviewsRDD.flatMap(x => x._2.getOrElse(List[Review]())).toDF().distinct().saveToMongodb(reviewsConfig.build)
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
