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

df = spark.read.option("multiline","true").json("hdfs://namenode:9000/bigdata_20241/stock_data/*.json")

# Biểu đồ nến theo ngày
from datetime import datetime, timedelta
from pyspark.sql.functions import to_date, date_format, col, unix_timestamp
import time

def get_candlestick_data(ticker_symbol):
    current_date = datetime.now().date()
    thirty_days_ago = current_date - timedelta(days=1000)
    
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
        .option("es.resource", "stock_candlestick_vic") \
        .mode("overwrite") \
        .save()

    return candlestick

get_candlestick_data("VIC")

print("Tất cả dữ liệu đã được đẩy vào Elasticsearch.")

spark.stop()