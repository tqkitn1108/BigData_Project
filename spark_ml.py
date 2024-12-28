# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, to_date
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("StockPrediction") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.jars", "/tmp/elasticsearch-spark-30_2.12-7.15.1.jar") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .getOrCreate()

def load_data(file_path):
    # Đọc dữ liệu JSON
    return spark.read.option("multiline", "true").json(file_path)

def prepare_data(df, ticker):
    # Lọc dữ liệu theo ticker
    df = df.filter(col("ticker") == ticker).dropna()
    
    # Chuyển đổi cột time sang kiểu ngày
    df = df.withColumn("date", to_date(col("time"), "yyyy-MM-dd'T'HH:mm:ss"))
    
    # Sắp xếp theo ngày
    df = df.orderBy('date')

    # Tạo đặc trưng thủ công từ các cột open, high, low, volume
    df = df.withColumn("feature_sum", col("open") + col("high") + col("low") + col("volume"))
    return df.select("ticker", "date", "feature_sum", col("close").alias("label"))

def train_model(df):
    # Tính toán trung bình và các giá trị cần thiết cho Linear Regression
    df = df.withColumn("x_mean", F.mean("feature_sum").over(Window.partitionBy()))
    df = df.withColumn("y_mean", F.mean("label").over(Window.partitionBy()))
    df = df.withColumn("numerator", (col("feature_sum") - col("x_mean")) * (col("label") - col("y_mean")))
    df = df.withColumn("denominator", (col("feature_sum") - col("x_mean")) ** 2)

    # Tính slope và intercept
    slope = df.agg(F.sum("numerator") / F.sum("denominator")).collect()[0][0]
    intercept = df.agg(F.mean("label") - slope * F.mean("feature_sum")).collect()[0][0]

    return slope, intercept

def predict_next_days(slope, intercept, df, ticker, num_days=30):
    # Lấy dòng cuối cùng làm cơ sở dự đoán
    last_row = df.orderBy(desc('date')).limit(1).collect()[0]
    last_feature = last_row['feature_sum']
    last_date = last_row['date']

    # Tạo dự đoán cho các ngày tiếp theo
    future_rows = [
        (ticker, str(last_date + F.expr("INTERVAL {} DAY".format(i))), last_feature, slope * last_feature + intercept)
        for i in range(1, num_days + 1)
    ]

    return spark.createDataFrame(future_rows, ["ticker", "date", "feature_sum", "predicted_close"])

def write_to_elasticsearch(df, index_name):
    # Ghi dữ liệu vào Elasticsearch
    df.write.format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .mode("overwrite").save(index_name)

def main():
    # Load dữ liệu
    df = load_data("hdfs://namenode:9000/bigdata_20241/stock_data/*.json")
    
    # Lấy danh sách mã cổ phiếu
    tickers = [row['ticker'] for row in df.select("ticker").distinct().collect()]

    for ticker in tickers:
        print("Processing ticker: {}".format(ticker))
        df_prepared = prepare_data(df, ticker)
        if df_prepared.count() > 0:  # Kiểm tra nếu có dữ liệu
            slope, intercept = train_model(df_prepared)
            predictions_df = predict_next_days(slope, intercept, df_prepared, ticker, num_days=30)
            index_name = "{}_predicted_prices".format(ticker.lower())
            write_to_elasticsearch(predictions_df, index_name)

            # Ghi kết quả vào file
            with open("predictions_{}_30days.txt".format(ticker), "w", encoding="utf-8") as f:
                f.write("Ticker | Date | Predicted_Close\n")
                for row in predictions_df.collect():
                    f.write("{} | {} | {}\n".format(row['ticker'], row['date'], row['predicted_close']))

if __name__ == "__main__":
    main()