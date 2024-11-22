from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, sum, lit, lag, first, last
from pyspark.sql.window import Window

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("StockDataProcessing") \
    .getOrCreate()

# Đọc dữ liệu từ Hadoop
file_path = "hdfs://path_to_your_data/stock_data.csv"  # Thay đường dẫn này
data = spark.read.csv(file_path, header=True, inferSchema=True)

# Xem trước dữ liệu
print("Dữ liệu ban đầu:")
data.show()

# Biểu đồ nến (Candlestick Chart)
candlestick_data = data.select("time", "open", "high", "low", "close") \
    .groupBy("time") \
    .agg(
        max("high").alias("high"),
        min("low").alias("low"),
        first("open").alias("open"),
        last("close").alias("close")
    )
print("Dữ liệu biểu đồ nến:")
candlestick_data.show()

# Biểu đồ khối lượng giao dịch (Volume Chart)
volume_data = data.groupBy("time").agg(sum("volume").alias("total_volume"))
print("Dữ liệu khối lượng giao dịch:")
volume_data.show()

# Biểu đồ đường (Line Chart)
line_data = data.select("time", "close").orderBy("time")
print("Dữ liệu biểu đồ đường:")
line_data.show()

# Thêm đường trung bình động (Moving Average - MA)
window_spec = Window.orderBy("time").rowsBetween(-6, 0)  # 7 ngày trung bình
line_data_with_ma = line_data.withColumn("MA_7", avg("close").over(window_spec))
print("Dữ liệu biểu đồ đường với trung bình động:")
line_data_with_ma.show()

# Biểu đồ ngành (Bar Chart hoặc Pie Chart)
industry_volume = data.groupBy("icb_name2").agg(sum("volume").alias("total_volume"))
print("Tổng khối lượng giao dịch theo ngành:")
industry_volume.show()

industry_close_avg = data.groupBy("icb_name2").agg(avg("close").alias("avg_close"))
print("Giá trị đóng cửa trung bình theo ngành:")
industry_close_avg.show()

# Biểu đồ phân tán (Scatter Plot)
scatter_data = data.select("volume", "close")
print("Dữ liệu biểu đồ phân tán:")
scatter_data.show()

# Biểu đồ tích lũy lợi nhuận (Cumulative Return Chart)
window_spec = Window.orderBy("time")

# Tính lợi nhuận hàng ngày
return_data = data.withColumn(
    "daily_return",
    (col("close") - lag("close", 1).over(window_spec)) / lag("close", 1).over(window_spec)
)

# Tính lợi nhuận tích lũy
return_data = return_data.withColumn(
    "cumulative_return",
    sum(lit(1) + col("daily_return")).over(window_spec) - lit(1)
)

print("Dữ liệu tích lũy lợi nhuận:")
return_data.select("time", "close", "daily_return", "cumulative_return").show()

# Biểu đồ histogram (Histogram)
daily_return_data = data.withColumn(
    "daily_return",
    (col("close") - lag("close", 1).over(window_spec)) / lag("close", 1).over(window_spec)
)
print("Dữ liệu phần trăm thay đổi giá (Daily Return) cho histogram:")
daily_return_data.select("daily_return").show()

# Lưu dữ liệu ra Hadoop
candlestick_output = "hdfs://path_to_your_output/candlestick_data"
candlestick_data.write.csv(candlestick_output, header=True)

volume_output = "hdfs://path_to_your_output/volume_data"
volume_data.write.csv(volume_output, header=True)

line_output = "hdfs://path_to_your_output/line_data_with_ma"
line_data_with_ma.write.csv(line_output, header=True)

industry_volume_output = "hdfs://path_to_your_output/industry_volume"
industry_volume.write.csv(industry_volume_output, header=True)

industry_close_avg_output = "hdfs://path_to_your_output/industry_close_avg"
industry_close_avg.write.csv(industry_close_avg_output, header=True)

scatter_output = "hdfs://path_to_your_output/scatter_data"
scatter_data.write.csv(scatter_output, header=True)

cumulative_return_output = "hdfs://path_to_your_output/cumulative_return_data"
return_data.select("time", "close", "daily_return", "cumulative_return") \
    .write.csv(cumulative_return_output, header=True)

daily_return_output = "hdfs://path_to_your_output/daily_return_data"
daily_return_data.select("daily_return").write.csv(daily_return_output, header=True)

print("Dữ liệu đã được xử lý và lưu thành công.")
