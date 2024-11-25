Hướng dẫn sử dụng

Project: Công cụ hỗ trợ phân tích thị trường chứng khoán

Cách thực hiện

Sử dụng:

- Chạy 'python ./stock_data_crawler.py' để crawl dữ liệu về máy
- Cấp quyền thực thi cho hdfs_saving.sh và run.sh: chmod +x ./run.sh và chmod +x ./hdfs_saving.sh
- Sau khi crawl xong dữ liệu về thư mục raw_data, chạy './hdfs_saving.sh' và hệ thống sẽ tự động lưu trữ liệu vào hdfs
- Sau khi lưu vào hdfs, chạy './run.sh' để chạy pyspark lưu vào elasticsearch
