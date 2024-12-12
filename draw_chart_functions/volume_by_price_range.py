from pyspark.sql.functions import col, when, count, lit

# Xác định các khoảng giá
df = df.withColumn(
    "price_range",
    when(col("close") <= 5, "0-5")
    .when((col("close") > 5) & (col("close") <= 10), "5-10")
    .when((col("close") > 10) & (col("close") <= 15), "10-15")
    .when((col("close") > 15) & (col("close") <= 20), "15-20")
    .when((col("close") > 20) & (col("close") <= 25), "20-25")
    .when((col("close") > 25) & (col("close") <= 30), "25-30")
    .when((col("close") > 30) & (col("close") <= 35), "30-35")
    .when((col("close") > 35) & (col("close") <= 40), "35-40")
    .when((col("close") > 40) & (col("close") <= 45), "40-45")
    .when((col("close") > 45) & (col("close") <= 50), "45-50")
    .otherwise(">50")
)

# Tính toán số lượng cổ phiếu theo khoảng giá
aggregated_df = df.groupBy("price_range") \
    .agg(count("*").alias("stock_count"))

# Thêm cột thứ tự sắp xếp cho nhãn để bảo toàn thứ tự khi lưu vào Elasticsearch
order_mapping = {
    "0-5": 1, "5-10": 2, "10-15": 3, "15-20": 4, "20-25": 5,
    "25-30": 6, "30-35": 7, "35-40": 8, "40-45": 9, "45-50": 10,
    ">50": 11
}

# Map thứ tự sắp xếp
aggregated_df = aggregated_df.withColumn(
    "order",
    when(col("price_range") == "0-5", lit(1))
    .when(col("price_range") == "5-10", lit(2))
    .when(col("price_range") == "10-15", lit(3))
    .when(col("price_range") == "15-20", lit(4))
    .when(col("price_range") == "20-25", lit(5))
    .when(col("price_range") == "25-30", lit(6))
    .when(col("price_range") == "30-35", lit(7))
    .when(col("price_range") == "35-40", lit(8))
    .when(col("price_range") == "40-45", lit(9))
    .when(col("price_range") == "45-50", lit(10))
    .otherwise(lit(11))
)

# Sắp xếp theo thứ tự đúng
sorted_df = aggregated_df.orderBy("order")

# Gửi dữ liệu lên Elasticsearch
sorted_df.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "volume_by_price_range") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .mode("overwrite") \
    .save()
