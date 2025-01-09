# Lọc dữ liệu liên quan đến mã "FPT" và chọn các cột cần thiết
filtered_data = df.filter(col("ticker") == "FPT") \
                  .select("time", "open", "close", "high", "low", "volume")

# Ghi dữ liệu ra file JSON trên HDFS
output_path = "hdfs://namenode:9000/output/stock_fpt_analysis.json"
filtered_data.write \
    .mode("overwrite") \
    .json(output_path)