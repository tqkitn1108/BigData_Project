from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

schema = StructType([
    StructField("time", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("ticker", StringType(), True),
    StructField("organ_name", StringType(), True),
    StructField("en_organ_name", StringType(), True),
    StructField("icb_name3", StringType(), True),
    StructField("en_icb_name3", StringType(), True),
    StructField("icb_name2", StringType(), True),
    StructField("en_icb_name2", StringType(), True),
    StructField("icb_name4", StringType(), True),
    StructField("en_icb_name4", StringType(), True),
    StructField("com_type_code", StringType(), True),
    StructField("icb_code1", StringType(), True),
    StructField("icb_code2", StringType(), True),
    StructField("icb_code3", StringType(), True),
    StructField("icb_code4", StringType(), True)
])


# Lọc các cột cần thiết: time, open, high, low, close, volume, ticker, icb_name2, icb_code2
# df_filtered = df.select(
#     "time", 
#     "open", 
#     "high", 
#     "low", 
#     "close", 
#     "volume", 
#     "ticker", 
#     "icb_name2", 
#     "icb_code2"
# )

# df_filtered.show()