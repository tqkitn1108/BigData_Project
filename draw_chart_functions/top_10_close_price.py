# Lấy top 10 mã cổ phiếu có giá "close" cao nhất
top_10_stocks = df.select("ticker", "close", "time") \
    .orderBy(col("close").desc()) \
    .limit(10)

# Định dạng dữ liệu để đưa vào Elasticsearch
# Elasticsearch yêu cầu dữ liệu dạng JSON key-value
formatted_data = top_10_stocks.withColumnRenamed("ticker", "stock_ticker") \
    .withColumnRenamed("close", "close_price") \
    .withColumnRenamed("time", "timestamp")

# Ghi dữ liệu vào Elasticsearch
formatted_data.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "top10_close_price") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .save()