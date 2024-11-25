from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Khởi tạo Spark
spark = SparkSession.builder \
    .appName("DailyStockAnalysis") \
    .getOrCreate()

# Xác định ngày hôm qua
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y_%m_%d')
file_path = f"hdfs://namenode:9000/data/stock_data/data_{yesterday}.json"

# Đọc dữ liệu từ file
yesterday_data = spark.read.json(file_path)

es_host = "elasticsearch:9200"
es_resource = "stocks_summary"

summary_data = spark.read \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", es_host) \
    .option("es.resource", es_resource) \
    .load()

# Thêm cột ngày cho dữ liệu hôm qua
yesterday_data = yesterday_data.withColumn("date", lit(yesterday))

# Gộp dữ liệu hôm qua và dữ liệu tổng hợp
combined_data = summary_data.union(yesterday_data)

# Lọc chỉ giữ lại 5 ngày gần nhất
recent_data = combined_data.orderBy(col("date").desc()).limit(5)

recent_data.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", es_host) \
    .option("es.resource", es_resource) \
    .mode("overwrite") \
    .save()
