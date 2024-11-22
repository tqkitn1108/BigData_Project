# # -*- coding: utf-8 -*-

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, desc
# import sys
# reload(sys)
# sys.setdefaultencoding('utf-8')

# # Tạo Spark session
# spark = SparkSession.builder \
#     .appName("HDFS Data Processing") \
#     .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
#     .config("spark.jars", "/tmp/elasticsearch-spark-30_2.12-7.15.1.jar") \
#     .config("spark.es.nodes", "elasticsearch") \
#     .config("spark.es.port", "9200") \
#     .getOrCreate()

# # Đọc file JSON từ HDFS
# # df = spark.read.schema(schema).json("hdfs://namenode:9000/data/test/stock_test/stock_test.json")
# df = spark.read.option("multiline","true").json("hdfs://namenode:9000/data/test/stock_test/stock_test.json")

# # Hiển thị dữ liệu ban đầu (tuỳ chọn, để kiểm tra)
# df.show()

# top_stocks = (
#     df.filter(col("volume").isNotNull())  # Lọc các dòng có volume khác null
#     .groupBy("ticker")  # Nhóm theo mã cổ phiếu (ticker)
#     .sum("volume")  # Tính tổng volume cho từng mã cổ phiếu
#     .withColumnRenamed("sum(volume)", "total_volume")  # Đổi tên cột
#     .orderBy(desc("total_volume"))  # Sắp xếp giảm dần theo volume
#     .limit(5)  # Lấy top 5
# )
# top_stocks.show()

# top_stocks.write \
#     .format("org.elasticsearch.spark.sql") \
#     .option("es.resource", "top_stocks") \
#     .option("es.nodes", "elasticsearch") \
#     .option("es.port", "9200") \
#     .mode("overwrite") \
#     .save()

# print("Dữ liệu đã được đẩy vào Elasticsearch.")

# # # Lưu kết quả vào HDFS trong thư mục output
# # # df_filtered.write.csv("hdfs://namenode:9000/data/output/stock_test_analysis/", header=True)
# # df_filtered.write.json("hdfs://namenode:9000/data/output/stock_test_analysis/", mode="overwrite")


# # # Dừng Spark session khi hoàn thành
# spark.stop()



# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, avg, max, min, sum, datediff, lag, round, to_date, date_format
from pyspark.sql.window import Window
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# Tạo Spark session
spark = SparkSession.builder \
    .appName("Stock Analysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.jars", "/tmp/elasticsearch-spark-30_2.12-7.15.1.jar") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .getOrCreate()

# Đọc file JSON
df = spark.read.option("multiline","true").json("hdfs://namenode:9000/data/test/stock_test/stock_test.json")

# 1. Dữ liệu cho Candlestick Chart
# candlestick_data = df.select(
#     "time", "ticker", "open", "high", "low", "close", "volume"
# ).orderBy("time")

# # Lưu vào Elasticsearch
# candlestick_data.write \
#     .format("org.elasticsearch.spark.sql") \
#     .option("es.resource", "stock_candlestick") \
#     .option("es.nodes", "elasticsearch") \
#     .option("es.port", "9200") \
#     .mode("overwrite") \
#     .save()

# 2. Phân tích Volume
volume_analysis = df.withColumn('date', to_date('time')) \
    .groupBy('date') \
    .agg(
        sum('volume').alias('total_daily_volume'),
        avg('volume').alias('avg_daily_volume')
    ) \
    .orderBy('date')

# Chuyển đổi định dạng ngày sang dd-MM-yyyy
volume_analysis = volume_analysis.withColumn(
    'date', 
    date_format('date', 'dd-MM-yyyy')
)

volume_analysis.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "stock_volume_test") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .mode("overwrite") \
    .save()

# 3. Phân tích biến động giá (% thay đổi)
# windowSpec = Window.partitionBy("ticker").orderBy("time")

# price_changes = df.select(
#     "time", 
#     "ticker",
#     "close",
#     lag("close").over(windowSpec).alias("prev_close")
# ) \
# .withColumn(
#     "price_change_pct", 
#     round(((col("close") - col("prev_close")) / col("prev_close") * 100), 2)
# ) \
# .na.drop()  # Loại bỏ null values

# price_changes.write \
#     .format("org.elasticsearch.spark.sql") \
#     .option("es.resource", "stock_price_changes") \
#     .option("es.nodes", "elasticsearch") \
#     .option("es.port", "9200") \
#     .mode("overwrite") \
#     .save()

# 4. Phân tích theo ngành
# industry_analysis = df.groupBy("icb_name2", "icb_name3", "icb_name4") \
#     .agg(
#         avg("close").alias("avg_price"),
#         avg("volume").alias("avg_volume"),
#         max("high").alias("highest_price"),
#         min("low").alias("lowest_price")
#     )

# industry_analysis.write \
#     .format("org.elasticsearch.spark.sql") \
#     .option("es.resource", "stock_industry") \
#     .option("es.nodes", "elasticsearch") \
#     .option("es.port", "9200") \
#     .mode("overwrite") \
#     .save()

# 5. Thống kê tổng hợp theo mã
# stock_summary = df.groupBy("ticker") \
#     .agg(
#         avg("close").alias("avg_price"),
#         max("high").alias("highest_price"),
#         min("low").alias("lowest_price"),
#         sum("volume").alias("total_volume"),
#         avg("volume").alias("avg_volume")
#     ) \
#     .orderBy(desc("total_volume"))

# stock_summary.write \
#     .format("org.elasticsearch.spark.sql") \
#     .option("es.resource", "stock_summary") \
#     .option("es.nodes", "elasticsearch") \
#     .option("es.port", "9200") \
#     .mode("overwrite") \
#     .save()

print("Tất cả dữ liệu đã được đẩy vào Elasticsearch.")

spark.stop()