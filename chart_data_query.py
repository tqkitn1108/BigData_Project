# Top 5 volume
# Bước 1: Tính tổng khối lượng giao dịch của từng mã cổ phiếu (ticker)
df_volume = df.groupBy("ticker").agg(
    sum("volume").alias("total_volume")
)

# Bước 2: Lọc ra top 5 mã cổ phiếu có khối lượng giao dịch lớn nhất
df_top_5 = df_volume.orderBy(desc("total_volume")).limit(5)

# Bước 3: Tính tổng khối lượng giao dịch toàn thị trường
total_market_volume = df.agg(sum("volume").alias("total_market_volume")).collect()[0]["total_market_volume"]

# Bước 4: Tính phần trăm khối lượng giao dịch của top 5 mã cổ phiếu
df_top_5_with_percentage = df_top_5.withColumn(
    "percentage", (col("total_volume") / total_market_volume) * 100
)

# Bước 5: Tính phần trăm tổng của top 5 mã cổ phiếu
total_top_5_percentage = df_top_5_with_percentage.agg(sum("percentage").alias("total_top_5_percentage")).collect()[0]["total_top_5_percentage"]

# Bước 6: Tính phần trăm của nhóm "Other" (các mã còn lại)
total_other_percentage = 100 - total_top_5_percentage

# Tạo một DataFrame cho nhóm "Other"
df_other_group = spark.createDataFrame([("Other", total_market_volume - df_top_5_with_percentage.agg(sum("total_volume")).collect()[0][0], total_other_percentage)], ["ticker", "total_volume", "percentage"])

# Bước 7: Kết hợp top 5 và nhóm "Other"
df_combined = df_top_5_with_percentage.union(df_other_group)

# Bước 8: Lưu dữ liệu vào Elasticsearch
df_combined.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "top_5_stock_volume") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .mode("overwrite") \
    .save()

# Phân tích giá trị giao dịch theo năm và sắp xếp
volume_analysis = df.withColumn('year', year('time')) \
    .withColumn('trading_value', col('volume') * col('close')) \
    .groupBy('year') \
    .agg(
        (sum('trading_value') / 1e9).alias('total_trading_value_billion'),
        (avg('trading_value') / 1e9).alias('avg_trading_value_billion')
    ) \
    .withColumn('year_sort', col('year')) \
    .withColumn('year', col('year').cast('string')) \
    .orderBy('year_sort')  # Sắp xếp theo cột year_sort

# Lưu kết quả vào Elasticsearch với mapping
volume_analysis.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "stock_trading_value_yearly") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .option("es.mapping.id", "year") \
    .option("es.write.data.type.mapping", """
    {
        "year": {"type": "keyword"},
        "year_sort": {"type": "integer"},
        "total_trading_value_billion": {"type": "double"},
        "avg_trading_value_billion": {"type": "double"}
    }
    """) \
    .mode("overwrite") \
    .save()