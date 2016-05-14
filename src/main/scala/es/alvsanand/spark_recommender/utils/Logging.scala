package es.alvsanand.spark_recommender.utils

import org.slf4j.LoggerFactory

/**
  * Created by alvsanand on 14/05/16.
  */
trait Logging {
  @transient
  protected val logger = LoggerFactory.getLogger(getClass().getName())
}
