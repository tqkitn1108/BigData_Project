#!/bin/bash

# docker compose up -d
# docker cp src spark-master:src

docker cp elasticsearch-spark-30_2.12-7.15.1.jar spark-master:/tmp/elasticsearch-spark-30_2.12-7.15.1.jar
docker cp analysis.py spark-master:/tmp/analysis.py

docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-master --conf spark.driver.port=5000 --conf spark.driver.bindAddress=0.0.0.0 --jars /tmp/elasticsearch-spark-30_2.12-7.15.1.jar /tmp/analysis.py

# docker exec -it spark-master /spark/bin/spark-submit --master spark://192.168.0.18:7077 \
#     --conf spark.driver.host=192.168.0.18 \
#     --conf spark.driver.port=5000 \
#     --conf spark.driver.bindAddress=0.0.0.0 \
#     --conf spark.driver.maxResultSize=2g \
#     --conf spark.network.timeout=800s \
#     --conf spark.rpc.message.maxSize=512 \
#     --conf spark.network.maxRemoteBlockSizeFetchToMem=200m \
#     --conf spark.executor.memory=4g \
#     --conf spark.executor.cores=2 \
#     --conf spark.driver.memory=4g \
#     --conf spark.broadcast.blockSize=128m \
#     --conf spark.sql.broadcastTimeout=1200 \
#     --jars /tmp/elasticsearch-spark-30_2.12-7.15.1.jar /tmp/analysis.py

# docker cp spark_ml.py spark-master:/tmp/spark_ml.py

# docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --jars /tmp/elasticsearch-spark-30_2.12-7.15.1.jar /tmp/spark_ml.py