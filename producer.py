import socket
import struct
import random
import string
import multiprocessing
import time
from collections import defaultdict

# Configuration
BROKER_NAMES = ["broker1","broker2","broker3","broker4","broker5"]
KEY = "a8eecf33-c18c-4d78-bf22-3770406e7768".encode('utf-8')
COMMAND = "PUSH".encode('utf-8')
SERVER_IP = '127.0.0.1'
SERVER_PORT = 8080

def generate_random_string(size):
    chars = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choice(chars) for _ in range(size)).encode('utf-8')

def send_push_message(broker_name, payload):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((SERVER_IP, SERVER_PORT))
            broker_name_bytes = broker_name.encode('utf-8')
            
            message = struct.pack(
                ">H{}sH{}sH{}s".format(len(KEY), len(COMMAND), len(broker_name_bytes)),
                len(KEY),
                KEY,
                len(COMMAND),
                COMMAND,
                len(broker_name_bytes),
                broker_name_bytes
            )
            message += payload

            message_length = len(message)
            s.sendall(struct.pack(">I", message_length) + message)

            response_length = struct.unpack(">I", s.recv(4))[0]
            s.recv(response_length)  # Read response (not used in this test)
        except Exception as e:
            print(f"Error sending message to broker {broker_name}: {e}")

def worker_process(broker_name, process_id, stats_queue):
    message_count = 0
    data_volume = 0

    start_time = time.time()

    while True:
        payload_size = random.randint(512, 1024 * 1024)  # Random payload size between 512 bytes and 1 MB
        payload = generate_random_string(payload_size)
        send_push_message(broker_name, payload)

        message_count += 1
        data_volume += payload_size

        current_time = time.time()
        if current_time - start_time >= 60:  # Report stats every minute
            stats_queue.put((process_id, broker_name, message_count, data_volume / (1024 * 1024)))  # Data volume in MB
            message_count = 0
            data_volume = 0
            start_time = current_time

def stats_collector(stats_queue, num_processes):
    stats = defaultdict(lambda: defaultdict(lambda: {"message_count": 0, "data_volume": 0}))
    overall_stats = defaultdict(lambda: {"message_count": 0, "data_volume": 0})
    finished_processes = set()

    while len(finished_processes) < num_processes:
        item = stats_queue.get()
        if item == "DONE":
            finished_processes.add(stats_queue.get())
            continue

        process_id, broker_name, message_count, data_volume = item
        stats[broker_name][process_id]["message_count"] = message_count
        stats[broker_name][process_id]["data_volume"] = data_volume

        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Broker Stats (Process {process_id}):")
        for broker, processes in stats.items():
            for pid, data in processes.items():
                print(f"  {broker} (Process {pid}): {data['message_count']} messages/s, {data['data_volume']:.2f} MB/s")

    # Calculate and print averages
    print("\nFinal Averages:")
    for broker, processes in stats.items():
        total_messages = 0
        total_data = 0
        for data in processes.values():
            total_messages += data["message_count"]
            total_data += data["data_volume"]
        overall_stats[broker]["message_count"] = total_messages / len(processes)
        overall_stats[broker]["data_volume"] = total_data / len(processes)
        print(f"  {broker}: {overall_stats[broker]['message_count']:.2f} messages/s, {overall_stats[broker]['data_volume']:.2f} MB/s")

if __name__ == "__main__":
    stats_queue = multiprocessing.Queue()
    num_processes = len(BROKER_NAMES) * 2

    processes = []
    for i, broker_name in enumerate(BROKER_NAMES):
        for j in range(2):  # 2 processes per broker
            process_id = i * 2 + j
            p = multiprocessing.Process(target=worker_process, args=(broker_name, process_id, stats_queue))
            processes.append(p)

    stats_process = multiprocessing.Process(target=stats_collector, args=(stats_queue, num_processes))
    stats_process.start()

    for p in processes:
        p.start()

    for p in processes:
        p.join()
        stats_queue.put("DONE")
        stats_queue.put(processes.index(p))

    stats_process.join()
