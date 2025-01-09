docker exec -it namenode bash -c "
  hadoop fs -get /output/stock_fpt_analysis.json /tmp/stock_fpt_analysis.json &&
  exit 
"
docker cp namenode:/tmp/stock_fpt_analysis.json ./stock_fpt_analysis.json