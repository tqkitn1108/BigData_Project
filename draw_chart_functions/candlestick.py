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


# {
#   "$schema": "https://vega.github.io/schema/vega-lite/v5.json",

#   "data": {
#     "url": {
#       "%context%": true,
#       "index": "stock_candlestick_aaa",  // Thay thế với tên index Elasticsearch của bạn
#       "body": {
#         "size": 10000,
#         "_source": ["timestamp", "open", "high", "low", "close", "display_date"]
#       }
#     },
#     "format": {"property": "hits.hits"}
#   },
#   "transform": [
#     {
#       "calculate": "toDate(datum._source.timestamp)",
#       "as": "date"
#     }
#   ],
#   "layer": [
#     {
#       "mark": {
#         "type": "rule",
#         "tooltip": true,
#         "strokeWidth": 2
#       },
#       "encoding": {
#         "x": {
#           "field": "date",
#           "type": "temporal",
#           "axis": {
#             "title": "Date",
#             "labelAngle": -45,
#             "labelFontSize": 12,
#             "titleFontSize": 14
#           }
#         },
#         "y": {
#           "field": "_source.low",
#           "type": "quantitative",
#           "title": "Price",
#           "axis": {
#             "labelFontSize": 12,
#             "titleFontSize": 14
#           }
#           ,
#   "scale": {
#     "domain": [110, 160]  // Set the focused range here
#   }
#         },
#         "y2": {"field": "_source.high"},
#         "color": {
#           "condition": {
#             "test": "datum._source.open < datum._source.close",
#             "value": "#2ca02c"  // Màu xanh lá cây đậm
#           },
#           "value": "#d62728"  // Màu đỏ đậm
#         },
#         "tooltip": [
#           {"field": "_source.display_date", "title": "Date"},
#           {"field": "_source.open", "title": "Open"},
#           {"field": "_source.close", "title": "Close"},
#           {"field": "_source.high", "title": "High"},
#           {"field": "_source.low", "title": "Low"}
#         ]
#       }
#     },
#     {
#       "mark": {
#         "type": "bar",
#         "width": 10
#       },
#       "encoding": {
#         "x": {
#           "field": "date",
#           "type": "temporal"
#         },
#         "y": {
#           "field": "_source.open",
#           "type": "quantitative"
#         },
#         "y2": {
#           "field": "_source.close"
#         },
#         "color": {
#           "condition": {
#             "test": "datum._source.open < datum._source.close",
#             "value": "#2ca02c"  // Màu xanh lá cây đậm
#           },
#           "value": "#d62728"  // Màu đỏ đậm
#         }
#       }
#     }
#   ]
# }