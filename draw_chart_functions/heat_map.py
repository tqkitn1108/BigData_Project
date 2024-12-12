# Tính tổng khối lượng giao dịch (volume) cho từng mã cổ phiếu
total_volume_per_ticker = df.groupBy("ticker").agg(
    sum("volume").alias("total_volume")
)

# Tính tổng khối lượng giao dịch toàn thị trường
total_market_volume = total_volume_per_ticker.agg(
    sum("total_volume").alias("total_market_volume")
).collect()[0]["total_market_volume"]

# Tính phần trăm tổng khối lượng giao dịch của từng mã cổ phiếu
volume_percentage_df = total_volume_per_ticker.withColumn(
    "percentage",
    round((col("total_volume") / total_market_volume), 4)
)

# Thêm cột ticker và phần trăm giao dịch vào DataFrame để đẩy lên Elasticsearch
es_data = volume_percentage_df.select("ticker", "total_volume", "percentage")

# Đẩy dữ liệu lên Elasticsearch
es_data.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "volume_heatmap") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .mode("overwrite") \
    .save()