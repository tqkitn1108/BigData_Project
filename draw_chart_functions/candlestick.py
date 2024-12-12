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

get_candlestick_data("AAA")