# 8. Dữ liệu cho Biểu đồ đường - Tỷ lệ % khối lượng giao dịch theo ngành qua các năm
from pyspark.sql.functions import (
    col, 
    desc, 
    avg, 
    max, 
    min, 
    sum, 
    lag, 
    round, 
    to_date, 
    date_format,
    concat,
    lit,
    year,
    unix_timestamp,
    sum as sum_col
)

# Tính tổng khối lượng theo năm và ngành
industry_volume_trend = df \
    .withColumn('year', date_format('time', 'yyyy')) \
    .groupBy('year', 'icb_name2') \
    .agg(sum('volume').alias('industry_volume'))

# Tính tổng khối lượng của tất cả ngành theo từng năm
windowSpec = Window.partitionBy('year')
industry_volume_trend = industry_volume_trend \
    .withColumn('total_year_volume', sum_col('industry_volume').over(windowSpec)) \
    .withColumn(
        'volume_percentage', 
        round((col('industry_volume') / col('total_year_volume') * 100), 2)
    ) \
    .select(
        'year',
        'icb_name2',
        'volume_percentage'
    ) \
    .orderBy('year', desc('volume_percentage'))

# Thêm thông tin hiển thị
industry_volume_trend = industry_volume_trend \
    .withColumn(
        'display_text',
        concat(
            col('icb_name2'),
            lit(': '),
            col('volume_percentage').cast('string'),
            lit('%')
        )
    )

industry_volume_trend.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "stock_industry_volume_trend") \
    .mode("overwrite") \
    .save()
