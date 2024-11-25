#!/bin/bash

# docker compose up -d
# docker cp src spark-master:src

# docker cp elasticsearch-spark-30_2.12-7.15.1.jar spark-master:/tmp/elasticsearch-spark-30_2.12-7.15.1.jar 
docker cp analysis.py spark-master:/tmp/analysis.py
# docker exec -it spark-master /spark/bin/spark-submit /tmp/analysis.py
docker exec -it spark-master /spark/bin/spark-submit --jars /tmp/elasticsearch-spark-30_2.12-7.15.1.jar /tmp/analysis.py