#!/bin/bash

# docker compose up -d
# docker cp src spark-master:src

docker cp elasticsearch-spark-30_2.12-7.15.1.jar spark-master:/tmp/elasticsearch-spark-30_2.12-7.15.1.jar
# docker cp analysis.py spark-master:/tmp/analysis.py
# docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --jars /tmp/elasticsearch-spark-30_2.12-7.15.1.jar /tmp/analysis.py

docker cp spark_ml.py spark-master:/tmp/spark_ml.py

docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --jars /tmp/elasticsearch-spark-30_2.12-7.15.1.jar /tmp/spark_ml.py

# --conf spark.driver.host=192.168.208.5