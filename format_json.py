import json

# Đọc file từ HDFS hoặc từ local
with open("stock_fpt.json", "r") as f:
    data = f.readlines()

# Chuyển từng dòng thành JSON object
json_objects = [json.loads(line) for line in data]

# Ghi vào file với định dạng hợp lệ (bọc trong mảng)
with open("stock_fpt_correct.json", "w") as f:
    json.dump(json_objects, f, indent=4)
