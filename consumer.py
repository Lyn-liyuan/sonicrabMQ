import socket
import struct
import time

# 全局变量，用于统计总接收字节数
total_bytes_received = 0

def recv_all(sock, length):
    """Ensure all data is received from the socket."""
    global total_bytes_received  # 使用全局变量
    data = b''
    while len(data) < length:
        packet = sock.recv(length - len(data))
        if not packet:
            raise ConnectionError("Connection closed prematurely")
        data += packet
    total_bytes_received += len(data)  # 累加接收的字节数
    return data

def fetch_messages(broker_name, offset):
    """Fetch messages from the broker."""
    new_offset = offset
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('127.0.0.1', 8080))
            
            # Prepare the request message
            command = "PULL".encode('utf-8')
            broker_name_bytes = broker_name.encode('utf-8')
            message = struct.pack(
                ">H{}sH{}sQ".format(len(command), len(broker_name_bytes)),
                len(command),
                command,
                len(broker_name_bytes),
                broker_name_bytes,
                offset
            )
            s.sendall(struct.pack(">I", len(message)) + message)
            
            while True:
                # Read response length
                response_length_bytes = recv_all(s, 4)
                response_length = struct.unpack(">I", response_length_bytes)[0]
                if response_length == 0:
                    print("No more messages.")
                    break
                
                # Read new offset
                new_offset_bytes = recv_all(s, 8)
                new_offset = struct.unpack(">Q", new_offset_bytes)[0]
                
                # Read message data
                total_data = recv_all(s, response_length)
                messages = total_data.decode('utf-8')  # Assuming the response is UTF-8 text
                print("Message Id:", new_offset, "| Message:", len(messages))
                global total_bytes_received
                print(f"Total bytes received so far: {total_bytes_received} bytes")
        return new_offset
    except Exception as e:
        print(f"Error: {e}")
    return -1

if __name__ == "__main__":
    broker_name = "test2_broker"
    offset = 0
    try:
        while True:
            result = fetch_messages(broker_name, offset)
            if result >= 0 :
                offset = result + 1

            print("Updated offset:", offset)
            time.sleep(2)
    except KeyboardInterrupt:
        print("Exiting...")
        print(f"Final total bytes received: {total_bytes_received} bytes")
