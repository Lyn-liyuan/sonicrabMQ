import socket
import struct
import random
import string


def send_push_message(broker_name, payload):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('127.0.0.1', 8080))
        
        command = "PUSH".encode('utf-8')
        broker_name_bytes = broker_name.encode('utf-8')
        
        message = struct.pack(
            ">H{}sH{}s".format(len(command), len(broker_name_bytes)),
            len(command),
            command,
            len(broker_name_bytes),
            broker_name_bytes
        )
        message += payload

        message_length = len(message)
        s.sendall(struct.pack(">I", message_length) + message)
        
        response_length = struct.unpack(">I", s.recv(4))[0]
        response = s.recv(response_length)
        #print("Server response:", response.decode('utf-8'))


def generate_random_string(size):
    chars = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choice(chars) for _ in range(size)).encode('utf-8')


if __name__ == "__main__":
    broker_name = "test_broker"
    for i in range(450):
        payload_size = random.randint(512, 1024 * 1024)  # 在512字节到1M字节之间随机生成负载大小
        payload = generate_random_string(payload_size)  # 生成指定大小的随机负载数据
        send_push_message(broker_name, payload)
        
        #if i % 1000 == 0:
        print(i)
        # time.sleep(1)
