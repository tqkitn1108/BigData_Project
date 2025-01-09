# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, to_date
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("StockPrediction") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.jars", "/tmp/elasticsearch-spark-30_2.12-7.15.1.jar") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .getOrCreate()

def load_data(file_path):
    return spark.read.option("multiline", "true").json(file_path)

def prepare_data(df, ticker):
    # Lọc dữ liệu theo mã cổ phiếu
    df = df.filter(col("ticker") == ticker).dropna()

    # Chuyển đổi cột time sang kiểu ngày
    df = df.withColumn("date", to_date(col("time"), "yyyy-MM-dd'T'HH:mm:ss"))

    # Sắp xếp theo ngày
    df = df.orderBy('date')

    # Tạo đặc trưng từ các cột open, high, low, volume
    features = ['open', 'high', 'low', 'volume']
    assembler = VectorAssembler(inputCols=features, outputCol='features')

    pipeline = Pipeline(stages=[assembler])
    df_prepared = pipeline.fit(df).transform(df)

    return df_prepared.select('ticker', 'date', 'features', col('close').alias('label'))

def train_model(df):
    # Huấn luyện mô hình tuyến tính (Linear Regression)
    lr = LinearRegression(featuresCol='features', labelCol='label')
    return lr.fit(df)

def predict_next_days(model, df, ticker, num_days=30):
    last_row = df.orderBy(desc('date')).limit(1).collect()[0]
    last_feature = last_row['features']
    last_date = last_row['date']

    # Tạo dự đoán cho các ngày tương lai
    future_rows = [
        (ticker, last_date + F.expr("INTERVAL {} DAYS".format(i)), last_feature, model.predict(last_feature))
        for i in range(1, num_days + 1)
    ]

    return spark.createDataFrame(future_rows, ["ticker", "date", "features", "predicted_close"])

def write_to_elasticsearch(df, index_name):
    df.write.format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .mode("overwrite").save(index_name)

def main():
    # Chỉ định ticker cụ thể
    specific_ticker = "FPT"

    # Tải dữ liệu
    df = load_data("hdfs://namenode:9000/bigdata_20241/stock_data/*.json")
    df_prepared = prepare_data(df, specific_ticker)

    if df_prepared.count() > 0:  # Ensure there's data for the ticker
        model = train_model(df_prepared)
        predictions_df = predict_next_days(model, df_prepared, specific_ticker, num_days=30)
        index_name = "{}_predicted_prices".format(specific_ticker.lower())
        write_to_elasticsearch(predictions_df, index_name)

        # Ghi dự đoán ra file
        with open("predictions_{}_30days.txt".format(specific_ticker), "w", encoding="utf-8") as f:
            f.write("Ticker | Date | Predicted_Close\n")
            for row in predictions_df.collect():
                f.write("{} | {} | {}\n".format(row['ticker'], row['date'], row['predicted_close']))

if __name__ == "__main__":
    main()