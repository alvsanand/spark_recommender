package es.alvsanand.spark_recommender.utils

/**
  * Created by alvsanand on 1/04/17.
  */
object HashUtils {
  def hash(string: String): Int = {
    string.hashCode  & 0xfffffff //Make it positive despite of collision
  }
}
