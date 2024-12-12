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
from pyspark.sql.functions import col, desc, avg, max, min, sum, datediff, lag, round, to_date, date_format, year, expr
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
# df = spark.read.option("multiline","true").json("hdfs://namenode:9000/data/test/stock_test/stock_test.json")
df = spark.read.option("multiline","true").json("hdfs://namenode:9000/bigdata_20241/stock_data/*.json")

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
from pyspark.sql.functions import (
    col, desc, datediff, current_timestamp, to_date, 
    when, lit, date_format, unix_timestamp
)
from datetime import datetime, timedelta

# Tạo dữ liệu xu hướng giá cho mã AAA trong 120 ngày gần nhất
def create_price_trend(ticker_symbol, days=120):
    current_date = datetime.now().date()
    start_date = current_date - timedelta(days=days)
    
    price_trend = df.filter(col("ticker") == ticker_symbol) \
        .withColumn("date", to_date("time")) \
        .filter(col("date") >= start_date) \
        .withColumn("price_trend", 
            when(col("close") >= col("open"), "up")
            .otherwise("down")
        ) \
        .withColumn("timestamp", 
            unix_timestamp("date").cast("long") * 1000
        ) \
        .withColumn("display_date", 
            date_format("date", "dd/MM/yyyy")
        ) \
        .select(
            "timestamp",
            "display_date",
            "open",
            "high",
            "low",
            "close",
            "price_trend"
        ) \
        .orderBy("timestamp")

    # Ghi dữ liệu vào Elasticsearch
    price_trend.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", "stock_price_trend_aaa") \
        .mode("overwrite") \
        .save()

    return price_trend

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, desc, round, year, max, first, last,
#     when, lit, concat, date_format
# )
# from pyspark.sql.window import Window
# import sys


# # Tạo DataFrame phân tích tăng trưởng
# stock_growth = df \
#     .withColumn("year", year(col("time"))) \
#     .groupBy("ticker", "year") \
#     .agg(
#         last("close").alias("year_end_price")
#     ) \
#     .filter((col("year") == 2022) | (col("year") == 2023))

# # Pivot data để có giá 2022 và 2023 trên cùng một dòng
# stock_growth_pivot = stock_growth \
#     .groupBy("ticker") \
#     .pivot("year", [2022, 2023]) \
#     .agg(first("year_end_price")) \
#     .withColumnRenamed("2022", "price_2022") \
#     .withColumnRenamed("2023", "price_2023")

# # Tính hiệu suất và làm tròn số
# final_growth = stock_growth_pivot \
#     .withColumn(
#         "growth_percentage",
#         round(((col("price_2023") - col("price_2022")) / col("price_2022") * 100), 2)
#     ) \
#     .withColumn(
#         "display_text",
#         concat(
#             col("ticker"),
#             lit(": "),
#             col("growth_percentage").cast("string"),
#             lit("%")
#         )
#     ) \
#     .orderBy(desc("growth_percentage")) \
#     .limit(10)  # Lấy top 10 cổ phiếu tăng mạnh nhất

# # Ghi dữ liệu vào Elasticsearch
# final_growth.write \
#     .format("org.elasticsearch.spark.sql") \
#     .option("es.resource", "stock_growth_analysis_2023") \
#     .mode("overwrite") \
#     .save()

print("Tất cả dữ liệu đã được đẩy vào Elasticsearch.")

spark.stop()