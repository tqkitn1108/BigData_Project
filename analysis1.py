# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, avg, max, min, sum, lag, round, to_date, date_format
from pyspark.sql.window import Window
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Stock Analysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.jars", "/tmp/elasticsearch-spark-30_2.12-7.15.1.jar") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .getOrCreate()

# Đọc dữ liệu từ HDFS
df = spark.read.option("multiline","true").json("hdfs://namenode:9000/data/test/stock_test/stock_test.json")

# 1. Dữ liệu cho Biểu đồ nến
candlestick_data = df.select(
    "time", "ticker", "open", "high", "low", "close", "volume"
).orderBy("time")

candlestick_data.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "stock_candlestick") \
    .mode("overwrite") \
    .save()

# 2. Dữ liệu cho Biểu đồ Volume
volume_analysis = df.withColumn('date', to_date('time')) \
    .groupBy('date') \
    .agg(
        sum('volume').alias('total_volume'),
        avg('volume').alias('avg_volume')
    ) \
    .orderBy('date')

volume_analysis = volume_analysis.withColumn(
    'date', 
    date_format('date', 'dd-MM-yyyy')
)

volume_analysis.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "stock_volume") \
    .mode("overwrite") \
    .save()

# 3. Dữ liệu cho Biểu đồ biến động giá
windowSpec = Window.partitionBy("ticker").orderBy("time")

price_changes = df.select(
    "time", 
    "ticker",
    "close",
    lag("close").over(windowSpec).alias("prev_close")
) \
.withColumn(
    "price_change_pct", 
    round(((col("close") - col("prev_close")) / col("prev_close") * 100), 2)
) \
.na.drop()

price_changes.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "stock_price_changes") \
    .mode("overwrite") \
    .save()

# 4. Dữ liệu cho Biểu đồ phân tích ngành
industry_analysis = df.groupBy("icb_name2", "icb_name3", "icb_name4") \
    .agg(
        avg("close").alias("avg_price"),
        avg("volume").alias("avg_volume"),
        max("high").alias("highest_price"),
        min("low").alias("lowest_price")
    )

industry_analysis.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "stock_industry") \
    .mode("overwrite") \
    .save()

# 5. Dữ liệu thống kê tổng hợp
stock_summary = df.groupBy("ticker") \
    .agg(
        avg("close").alias("avg_price"),
        max("high").alias("highest_price"),
        min("low").alias("lowest_price"),
        sum("volume").alias("total_volume"),
        avg("volume").alias("avg_volume")
    ) \
    .orderBy(desc("total_volume"))

stock_summary.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "stock_summary") \
    .mode("overwrite") \
    .save()

print("Đã hoàn thành việc xử lý và gửi dữ liệu lên Elasticsearch")

spark.stop()