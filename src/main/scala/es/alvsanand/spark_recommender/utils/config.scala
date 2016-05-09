package es.alvsanand.spark_recommender.utils

case class MongoConfig(val hosts: String, val db: String)

case class ESConfig(val hosts: String, val index: String)