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

# 3. Dữ liệu cho Biểu đồ biến động giá 30 ngày gần nhất
from datetime import datetime, timedelta
from pyspark.sql.functions import to_date, date_format, col, lag, round, current_timestamp, unix_timestamp
from pyspark.sql.window import Window

def get_price_changes_30days(ticker_symbol):
    current_date = datetime.now().date()
    thirty_days_ago = current_date - timedelta(days=90)
    
    windowSpec = Window.partitionBy("ticker").orderBy("date")
    
    price_changes = df.filter(col("ticker") == ticker_symbol) \
        .withColumn("date", to_date("time")) \
        .filter(
            (col("date") >= thirty_days_ago) & 
            (col("date") <= current_date)
        ) \
        .select(
            "date",
            "ticker",
            "close",
            lag("close").over(windowSpec).alias("prev_close")
        ) \
        .withColumn(
            "price_change_pct", 
            round(((col("close") - col("prev_close")) / col("prev_close")), 2)
        ) \
        .na.drop() \
        .withColumn(
            "timestamp", 
            unix_timestamp("date").cast("long") * 1000  # Convert to milliseconds for Kibana
        ) \
        .withColumn(
            "display_date", 
            date_format("date", "dd/MM/yyyy")
        ) \
        .orderBy("date") \
        .select(
            "timestamp",     # Dùng để sort
            "display_date",  # Dùng để hiển thị
            "close",
            "price_change_pct"
        )

    price_changes.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", "stock_price_changes_aas_30d") \
        .mode("overwrite") \
        .save()

    return price_changes

# Sử dụng hàm
# get_price_changes_30days("AAS")

# Biểu đồ nến theo ngày
from datetime import datetime, timedelta
from pyspark.sql.functions import to_date, date_format, col, unix_timestamp

def get_candlestick_data(ticker_symbol):
    current_date = datetime.now().date()
    thirty_days_ago = current_date - timedelta(days=90)
    
    candlestick = df.filter(col("ticker") == ticker_symbol) \
        .withColumn("date", to_date("time")) \
        .filter(
            (col("date") >= thirty_days_ago) & 
            (col("date") <= current_date)
        ) \
        .select(
            "date",
            "open",
            "high", 
            "low",
            "close",
            "volume"
        ) \
        .withColumn(
            "timestamp", 
            unix_timestamp("date").cast("long") * 1000
        ) \
        .withColumn(
            "display_date", 
            date_format("date", "dd/MM/yyyy")
        ) \
        .orderBy("date") \
        .select(
            "timestamp",     # Dùng để sort
            "display_date",  # Hiển thị ngày
            "open",         # Giá mở cửa
            "high",         # Giá cao nhất
            "low",          # Giá thấp nhất
            "close",        # Giá đóng cửa
            "volume"        # Khối lượng giao dịch
        )

    candlestick.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", "stock_candlestick_aaa_v2") \
        .mode("overwrite") \
        .save()

    return candlestick

# get_candlestick_data("AAA")

# volume_analysis = df.withColumn('year', year('time')) \
#     .withColumn('trading_value', col('volume') * col('close') /1e9) \
#     .groupBy('year') \
#     .agg(
#         (sum('trading_value')).alias('total_trading_value_billion')
#     ) \
#     .withColumn('year_sort', col('year')) \
#     .withColumn('year', col('year').cast('string')) \
#     .orderBy('year_sort')  # Sắp xếp theo cột year_sort

volume_analysis = df.withColumn("year", year("time")) \
    .withColumn("transaction_value", (col("close") * col("volume")) / 1e9) \
    .groupBy("year") \
    .agg(sum("transaction_value").alias("total_trading_value_billion"))

# Lưu kết quả vào Elasticsearch với mapping
volume_analysis.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "stock_trading_value_yearly") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .option("es.mapping.id", "year") \
    .mode("overwrite") \
    .save()

print("Tất cả dữ liệu đã được đẩy vào Elasticsearch.")

spark.stop()