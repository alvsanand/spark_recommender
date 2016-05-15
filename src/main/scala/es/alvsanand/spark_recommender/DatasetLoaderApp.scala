package es.alvsanand.spark_recommender

import es.alvsanand.spark_recommender.parser.{DatasetDownloader, DatasetIngestion}
import es.alvsanand.spark_recommender.utils.{ESConfig, Logging, MongoConfig}
import org.apache.spark.SparkConf
import scopt.OptionParser

/**
  * @author ${user.name}
  */
object DatasetLoaderApp extends App with Logging {

  override def main(args: Array[String]) {
    val defaultParams = scala.collection.mutable.Map[String, Any]()
    defaultParams += "spark.cores" -> "local[*]"
    defaultParams += "spark.option" -> scala.collection.mutable.Map[String, String]()
    defaultParams += "mongo.hosts" -> "127.0.0.1:27017"
    defaultParams += "mongo.db" -> "spark_recommender"
    defaultParams += "es.httpHosts" -> "127.0.0.1:9200"
    defaultParams += "es.transportHosts" -> "127.0.0.1:9300"
    defaultParams += "es.index" -> "spark_recommender"
    defaultParams += "dataset.tmp.dir" -> "%s/.spark_recommender".format(sys.env("HOME"))

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
      opt[String]("es.httpHosts")
        .text("ElasicSearch HTTP Hosts")
        .action((x, c) => {
          c += "es.httpHosts" -> x
        })
      opt[String]("es.transportHosts")
        .text("ElasicSearch Transport Hosts")
        .action((x, c) => {
          c += "es.transportHosts" -> x
        })
      opt[String]("es.index")
        .text("ElasicSearch index")
        .action((x, c) => {
          c += "es.index" -> x
        })
      opt[String]("dataset.tmp.dir")
        .text("Temporal directory to store the products dataset")
        .action((x, c) => {
          c += "dataset.tmp.dir" -> x
        })
      opt[String]("dataset.file")
        .text("Ingest only one dataset file")
        .action((x, c) => {
          c += "dataset.file" -> x
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
    implicit val esConf = new ESConfig(params("es.httpHosts").asInstanceOf[String], params("es.transportHosts").asInstanceOf[String], params("es.index").asInstanceOf[String])


    try {
      DatasetDownloader.download(params("dataset.tmp.dir").asInstanceOf[String])
      DatasetIngestion.storeData(DatasetDownloader.getFinalDstName(params("dataset.tmp.dir").asInstanceOf[String]), Option(params.getOrElse("dataset.file", null).asInstanceOf[String]))
    }
    catch {
      case e: Exception =>
        logger.error("Error executing DatasetLoaderApp", e)
        sys.exit(1)
    }

    sys.exit(0)
  }
}
