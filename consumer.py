import socket
import struct
import time
import multiprocessing
from collections import defaultdict

# Configuration
BROKER_NAMES = ["broker1","broker2","broker3","broker4","broker5"]
SERVER_IP = '127.0.0.1'
SERVER_PORT = 8080
KEY = "a8eecf33-c18c-4d78-bf22-3770406e7768".encode('utf-8')
COMMAND = "PULL".encode('utf-8')

# Global variable for tracking total bytes received
total_bytes_received = multiprocessing.Value('i', 0)

def recv_all(sock, length):
    """Ensure all data is received from the socket."""
    data = b''
    while len(data) < length:
        packet = sock.recv(length - len(data))
        if not packet:
            raise ConnectionError("Connection closed prematurely")
        data += packet
    return data

def fetch_messages(broker_name, offset, stats_queue, total_bytes_received):
    """Fetch messages from the broker."""
    message_count = 0
    data_volume = 0
    
    start_time = time.time()
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((SERVER_IP, SERVER_PORT))

                while True:
                    # Prepare the request message
                    broker_name_bytes = broker_name.encode('utf-8')
                    message = struct.pack(
                        ">H{}sH{}sH{}sQ".format(len(KEY), len(COMMAND), len(broker_name_bytes)),
                        len(KEY),
                        KEY,
                        len(COMMAND),
                        COMMAND,
                        len(broker_name_bytes),
                        broker_name_bytes,
                        offset
                    )
                    s.sendall(struct.pack(">I", len(message)) + message)
                    
                    received_len = 0
                    new_offset_bytes = None
                    while True:
                        # Read response length
                        response_length_bytes = recv_all(s, 4)
                        response_length = struct.unpack(">I", response_length_bytes)[0]
                        
                        if response_length == 0:
                            if new_offset_bytes != None:
                               offset = offset + 1
                            
                            break  # No more messages in this batch
                        
                        
                            
                        # Read new offset
                        new_offset_bytes = recv_all(s, 8)
                        offset = struct.unpack(">Q", new_offset_bytes)[0]
                        
                        if received_len > 1024*1024*100:
                            print(f"Error: message {offset} length error!! length:{received_len}")
                            break
                        # Read message data
                        total_data  = recv_all(s, response_length)
                        received_len += len(total_data)
                        message_count += 1
                        
                    
                    data_volume += received_len
                    
                    # Update global bytes received
                    with total_bytes_received.get_lock():
                        total_bytes_received.value += received_len

                    current_time = time.time()
                    if current_time - start_time >= 60:  # Report stats every minute
                        stats_queue.put((broker_name, message_count, data_volume / (1024 * 1024)))  # Data volume in MB
                        message_count = 0
                        data_volume = 0
                        start_time = current_time

        except Exception as e:
            #print(f"Error fetching messages from broker {broker_name}: {e}")
            time.sleep(1)

def stats_collector(stats_queue, num_processes):
    """Collect and print statistics from all processes."""
    stats = defaultdict(lambda: {"message_count": 0, "data_volume": 0})
    finished_processes = 0

    while finished_processes < num_processes:
        item = stats_queue.get()
        if item == "DONE":
            finished_processes += 1
            continue

        broker_name, message_count, data_volume = item
        stats[broker_name]["message_count"] += message_count
        stats[broker_name]["data_volume"] += data_volume

        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Broker Stats:")
        for broker, data in stats.items():
            print(f"  {broker}: {data['message_count']} messages/s, {data['data_volume']:.2f} MB/s")

    print("\nFinal Averages:")
    for broker, data in stats.items():
        print(f"  {broker}: {data['message_count'] / num_processes:.2f} messages/s, {data['data_volume'] / num_processes:.2f} MB/s")

if __name__ == "__main__":
    stats_queue = multiprocessing.Queue()
    num_processes = len(BROKER_NAMES)

    processes = []
    for broker_name in BROKER_NAMES:
        offset = 0
        p = multiprocessing.Process(target=fetch_messages, args=(broker_name, offset, stats_queue, total_bytes_received))
        processes.append(p)

    stats_process = multiprocessing.Process(target=stats_collector, args=(stats_queue, num_processes))
    stats_process.start()

    for p in processes:
        p.start()

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("Stopping processes...")
    finally:
        for p in processes:
            p.terminate()
            stats_queue.put("DONE")

    stats_process.join()

    print(f"Final total bytes received: {total_bytes_received.value / (1024 * 1024):.2f} MB")
