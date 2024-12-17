import struct

# 定义每条记录头部的大小和结构
HEADER_SIZE = 12
HEADER_FORMAT = '>IQ'  # > 表示大端模式, I 表示 4 字节无符号整数, Q 表示 8 字节无符号整数

def read_record_by_position(file_path, target_position):
    with open(file_path, 'rb') as f:
        while True:
            # 读取头部数据
            header = f.read(HEADER_SIZE)
            if not header:
                print("Position not found in the file.")
                break

            # 解析头部数据
            data_len, position = struct.unpack(HEADER_FORMAT, header)

            # 检查是否是目标位置
            if position == target_position:
                data = f.read(data_len)
                print(f"Data length: {data_len}")
                print(f"Actully Data length: {len(data)}")
                return

            # 跳过当前记录的数据部分
            f.seek(data_len, 1)

# 示例调用
file_path = "/disk/lyn/workspace/esw/messages/test_broker/000000000200.data"  # 替换为你的文件路径
target_position = 400  # 替换为你要查询的位置编号
read_record_by_position(file_path, target_position)
