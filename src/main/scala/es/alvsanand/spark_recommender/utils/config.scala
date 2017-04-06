package es.alvsanand.spark_recommender.utils

case class MongoConfig(val uri: String, val db: String)

case class ESConfig(val httpHosts: String, val transportHosts: String, val index: String)