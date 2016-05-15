package es.alvsanand.spark_recommender

import es.alvsanand.spark_recommender.trainer.ALSTrainer
import es.alvsanand.spark_recommender.utils.{Logging, MongoConfig}
import org.apache.spark.SparkConf
import scopt.OptionParser

/**
  * @author ${user.name}
  */
object RecommenderTrainerApp extends App with Logging {

  override def main(args: Array[String]) {
    val defaultParams = scala.collection.mutable.Map[String, Any]()
    defaultParams += "spark.cores" -> "local[*]"
    defaultParams += "spark.option" -> scala.collection.mutable.Map[String, String]()
    defaultParams += "mongo.hosts" -> "127.0.0.1:27017"
    defaultParams += "mongo.db" -> "spark_recommender"
    defaultParams += "maxRecommendations" -> ALSTrainer.MAX_RECOMMENDATIONS.toString

    val parser = new OptionParser[scala.collection.mutable.Map[String, Any]]("ScaleDataset") {
      head("Spark Recommender Example")
      opt[String]("spark.cores")
        .text("Number of cores in the Spark cluster")
        .action((x, c) => {
          c += "spark.cores" -> x
        })
      opt[Map[String,String]]("spark.option")
        .text("Spark Config Option")
        .valueName("spark.property1=value1,spark.property2=value2,...")
        .action { (x, c) => {
          c("spark.option").asInstanceOf[scala.collection.mutable.Map[String, Any]] ++= x.toSeq
          c
        }
        }
      opt[String]("mongo.hosts")
        .text("Mongo Hosts")
        .action((x, c) => {
          c += "mongo.hosts" -> x
        })
      opt[String]("mongo.db")
        .text("Mongo Database")
        .action((x, c) => {
          c += "mongo.db" -> x
        })
      opt[String]("maxRecommendations")
        .text("Maximum number of recommendations")
        .action((x, c) => {
          c += "maxRecommendations" -> x
        })
      help("help") text("prints this usage text")
    }
    parser.parse(args, defaultParams).map { params =>
      run(params.toMap)
    } getOrElse {
      System.exit(1)
    }
  }

  private def run(params: Map[String, Any]): Unit = {
    implicit val conf = new SparkConf().setAppName("RecommenderTrainerApp").setMaster(params("spark.cores").asInstanceOf[String])
    params("spark.option").asInstanceOf[scala.collection.mutable.Map[String, Any]].foreach { case (key: String, value: String) => conf.set(key, value) }

    implicit val mongoConf = new MongoConfig(params("mongo.hosts").asInstanceOf[String], params("mongo.db").asInstanceOf[String])
    val maxRecommendations = params("maxRecommendations").asInstanceOf[String].toInt

    try {
      ALSTrainer.calculateRecs(maxRecommendations)
    }
    catch {
      case e: Exception =>
        logger.error("Error executing RecommenderTrainerApp", e)
        sys.exit(1)
    }

    sys.exit(0)
  }
}
