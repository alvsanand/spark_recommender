package es.alvsanand.spark_recommender

import es.alvsanand.spark_recommender.trainer.ALSTrainer
import es.alvsanand.spark_recommender.utils.MongoConfig
import org.apache.spark.SparkConf
import scopt.OptionParser

/**
 * @author ${user.name}
 */
object RecommenderTrainerApp extends App {

  override def main(args : Array[String]) {
    val defaultParams = scala.collection.mutable.Map[String, String]()
    defaultParams += "spark.cores" -> "local[*]"
    defaultParams += "mongo.hosts" -> "127.0.0.1:27017"
    defaultParams += "mongo.db" -> "spark_recommender"
    defaultParams += "maxRecommendations" -> "100"

    val parser = new OptionParser[scala.collection.mutable.Map[String, String]]("ScaleDataset"){
      head("Spark Recommender Example")
      opt[String]("spark.cores")
        .text("Number of cores in the Spark cluster")
        .action((x: String, c) => { c += "spark.cores" -> x })
      opt[String]("mongo.hosts")
        .text("Mongo Hosts")
        .action((x: String, c) => { c += "mongo.hosts" -> x })
      opt[String]("mongo.db")
        .text("Mongo Database")
        .action((x: String, c) => { c += "mongo.db" -> x })
      opt[String]("maxRecommendations")
        .text("Maximum number of recommendations")
        .action((x: String, c) => { c += "maxRecommendations" -> x })
    }
    parser.parse(args, defaultParams).map { params =>
      run(params.toMap)
    } getOrElse {
      System.exit(1)
    }
  }

  private def run(params: Map[String, String]): Unit = {
    implicit val conf = new SparkConf().setAppName("RecommenderTrainerApp").setMaster(params("spark.cores"))
    implicit val mongoConf = new MongoConfig(params("mongo.hosts"), params("mongo.db"))
    val maxRecommendations = params("maxRecommendations").toInt

    try{
      ALSTrainer.calculateRecs(maxRecommendations)
    }
    catch {
      case e: Exception =>
        println("Error executing RecommenderTrainerApp")
        println(e)
        sys.exit(1)
    }

    sys.exit(0)
  }
}
