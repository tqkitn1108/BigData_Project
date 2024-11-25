docker cp ./raw_data namenode:/stock_data
docker exec -it namenode bash -c "
  hdfs dfs -mkdir -p /bigdata_20241/stock_data &&
  hdfs dfs -put /stock_data/* /bigdata_20241/stock_data &&
  hdfs dfs -ls /bigdata_20241/stock_data
"