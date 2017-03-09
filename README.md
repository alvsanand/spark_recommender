# spark_recommender
This repository is a just Proof of Cncept \(POC\) of how to create a Product Recommender using the latest Big Data technologies such as [Apache Spark](http://spark.apache.org) and [Elasticsearch](https://www.elastic.co/products/elasticsearch).

It is very advisable to read my two articles that refers to this PoC where you can find some theory behind the recommenders and more technical detail:

* [Creating a Recommender System Part I](http://blog.stratio.com/creating-a-recommender-system-part-i/) 
* [Creating a Recommender System Part II](http://blog.stratio.com/creating-recommender-system-part-two/) 

# Technical Requirements

In order to launch this Poc, you must have running:

* A MongoDB ReplicaSet/Single Instance.
* An Elasticsearch Cluster/Single Instance.

## How to compile it

Just run the following command:

```
mvn clean compile 
```

## How to run it

This Poc are split in two main parts:

* RecommenderTrainerApp: pre-calculates the recommendations.
* RecommenderServerApp: return the recommendations.

## es.alvsanand.spark_recommender.RecommenderTrainerApp

This process is the responsible of:
 
* Downloads the dataset.
* Reads and parsing the product catalog and user ratings using Apache Spark \(SparkSQL\).
* Stores the catalog/ratings into the databases (MongoDB and Elasticsearch).
* Trains the Collaborative Filtering (CF) model using the [ALS algorithm](https://bugra.github.io/work/notes/2014-04-19/alternating-least-squares-method-for-collaborative-filtering/) using Apache Spark \(MLlib\).
* Pre-calculates CF recommendations (User-Product and Product-Product) and saving them into DB (MongoDB).

> This PoC use this [Amazon Dataset](http://times.cs.uiuc.edu/~wang296/Data/LARA/Amazon/readme.txt).

### How to launch trainer

Just run the following command:

```
mvn exec:java -Dexec.mainClass="es.alvsanand.spark_recommender.RecommenderTrainerApp" -Dexec.args=""
```

* These are its parameters:

```
Recommendation System Trainer
Usage: RecommenderTrainerApp [options]

  --spark.cores <value>
        Number of cores in the Spark cluster
  --spark.option spark.property1=value1,spark.property2=value2,...
        Spark Config Option
  --mongo.hosts <value>
        Mongo Hosts
  --mongo.db <value>
        Mongo Database
  --maxRecommendations <value>
        Maximum number of recommendations
  --help
        prints this usage text
```

## es.alvsanand.spark_recommender.RecommenderServerApp

It is a REST API server that returns product recommendations. This PoC is able to return the following types of recommendations:

* Collaborative Filtering:
    * User-Product:
    
    ```curl -H "Content-Type: application/json" -XPOST 'localhost:8080/recs/cf/user' -d '{"userId": "Adam C. Kauffman"}'```
    * Product-Product:
    
    ```curl -H "Content-Type: application/json" -XPOST 'localhost:8080/recs/cf/product' -d '{"productId": "B00E3JJZ5U"}'```
* Content Based:
    * Search Based:
                      
    ```curl -H "Content-Type: application/json" -XPOST 'localhost:8080/recs/cb/sch' -d '{"text": "MY TEXT"}'```
    * Similar Product:
                         
    ```curl -H "Content-Type: application/json" -XPOST 'localhost:8080/recs/cb/mrl' -d '{"productId": "B00E3JJZ5U"}'```
* Hybrid Recommendations (Product-Product CF and Similar Product CB):
                                                                                             
    ```curl -H "Content-Type: application/json" -XPOST 'localhost:8080/recs/hy/pro' -d '{"productId": "B00E3JJZ5U"}'```

### How to launch the server

Just run the following command:

```
mvn exec:java -Dexec.mainClass="es.alvsanand.spark_recommender.RecommenderServerApp" -Dexec.args="--help"
```

* These are its parameters:

```
Recommendation System Server
Usage: RecommenderServerApp [options]

  --server.port <value>
        HTTP server port
  --mongo.hosts <value>
        Mongo Hosts
  --mongo.db <value>
        Mongo Database
  --es.httpHosts <value>
        ElasicSearch HTTP Hosts
  --es.transportHosts <value>
        ElasicSearch Transport Hosts
  --es.index <value>
        ElasicSearch index
  --help
        prints this usage text
```