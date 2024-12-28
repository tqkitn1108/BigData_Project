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
            date_format("date", "yyyy/MM/dd")
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

get_price_changes_30days("AAS")