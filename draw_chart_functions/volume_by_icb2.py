# 6. Dữ liệu cho Biểu đồ Pie Chart - Phân tích khối lượng giao dịch theo ngành
industry_volume = df.groupBy("icb_name2") \
    .agg(
        sum("volume").alias("total_volume")
    ) \
    .filter(col("total_volume") > 0) \
    .orderBy(desc("total_volume"))

industry_volume.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "stock_industry_volume") \
    .mode("overwrite") \
    .save()