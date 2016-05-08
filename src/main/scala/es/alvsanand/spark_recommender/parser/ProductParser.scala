package es.alvsanand.spark_recommender.parser

import org.apache.spark.sql.types._

/**
  * Created by alvsanand on 7/05/16.
  */
object ProductParser {
  private val DATASET_URL = "http://times.cs.uiuc.edu/~wang296/Data/LARA/Amazon/AmazonReviews.zip"

  private val DATASET_NAME = "%s/AmazonReviews.zip"
  private val TMP_DATASET_NAME = "%s/AmazonReviews.zip.tmp"
  private val FINAL_DATASET_NAME = "%s/AmazonReviews"

  private def reviewStructure(): List[StructField] = {
    List(
      StructField("Title", StringType),
      StructField("Author", StringType),
      StructField("ReviewID", StringType),
      StructField("Overall", DoubleType),
      StructField("Content", StringType),
      StructField("Date", StringType)
    )
  }

  private def productStructure(): List[StructField] = {
    List(
      StructField("Price", StringType),
      StructField("Features", StringType),
      StructField("Name", StringType),
      StructField("ImgURL", StringType),
      StructField("ProductID", StringType)
    )
  }

  def productReviewStructure(): StructType = {
    StructType(
      StructField("Reviews", ArrayType(StructType(reviewStructure)))
      :: StructField("ProductInfo", StructType(productStructure))
      :: Nil
    )
  }
}
