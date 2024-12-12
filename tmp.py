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
es.mapping.id