# spark_recommender
Spark Recommender example


    curl -H "Content-Type: application/json" -XPOST 'localhost:8080/recs/cf/user' -d '{"userId": "Adam C. Kauffman"}'
    curl -H "Content-Type: application/json" -XPOST 'localhost:8080/recs/cf/product' -d '{"productId": "B00E3JJZ5U"}'