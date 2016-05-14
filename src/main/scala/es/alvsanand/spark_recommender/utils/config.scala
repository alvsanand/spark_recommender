package es.alvsanand.spark_recommender.utils

case class MongoConfig(val hosts: String, val db: String)

case class ESConfig(val httpHosts: String, val transportHosts: String, val index: String)