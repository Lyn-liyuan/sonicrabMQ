import socket
import struct

PUSH_COMMAND = b'PUSH'
PULL_COMMAND = b'PULL'

class Client:
    def __init__(self, server_ip, server_port, key):
        self.server_ip = server_ip
        self.server_port = server_port
        self.key = key.encode("utf-8")
        self.connection = None  # Maintain a persistent connection

    def connect(self):
        """Establish a persistent connection if not already connected."""
        if self.connection is None:
            try:
                self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.connection.connect((self.server_ip, self.server_port))
            except Exception as e:
                self.connection = None
                raise ConnectionError(f"Failed to connect to server: {e}")

    def close(self):
        """Close the persistent connection."""
        if self.connection:
            try:
                self.connection.close()
            except Exception as e:
                print(f"Error closing connection: {e}")
            finally:
                self.connection = None

    def send_push_message(self, broker_name, payload):
        """Send a message to the broker."""
        try:
            self.connect()
            broker_name_bytes = broker_name.encode('utf-8')

            # Construct message
            message = struct.pack(
                ">H{}sH{}sH{}s".format(len(self.key), len(PUSH_COMMAND), len(broker_name_bytes)),
                len(self.key),
                self.key,
                len(PUSH_COMMAND),
                PUSH_COMMAND,
                len(broker_name_bytes),
                broker_name_bytes
            )
            message += payload

            # Send message length and message
            message_length = len(message)
            self.connection.sendall(struct.pack(">I", message_length) + message)

            # Receive response length
            response_length = struct.unpack(">I", self.recv_all(4))[0]
            response = self.recv_all(response_length)  # Read response (if needed)
            return response

        except Exception as e:
            print(f"Error sending message to broker {broker_name}: {e}")

    def fetch_messages(self, broker_name, offset):
        """Fetch messages from the broker."""
        try:
            self.connect()

            # Prepare the request message
            broker_name_bytes = broker_name.encode('utf-8')
            message = struct.pack(
                ">H{}sH{}sH{}sQ".format(len(self.key), len(PULL_COMMAND), len(broker_name_bytes)),
                len(self.key),
                self.key,
                len(PULL_COMMAND),
                PULL_COMMAND,
                len(broker_name_bytes),
                broker_name_bytes,
                offset
            )
            self.connection.sendall(struct.pack(">I", len(message)) + message)

            # Read response length
            response_length_bytes = self.recv_all(4)
            response_length = struct.unpack(">I", response_length_bytes)[0]

            if response_length == 0:
                return None  # No more messages

            # Read new offset
            new_offset_bytes = self.recv_all(8)
            new_offset = struct.unpack(">Q", new_offset_bytes)[0]

            # Read message data
            message_data = self.recv_all(response_length)

            return new_offset, message_data

        except Exception as e:
            print(f"Error fetching messages from broker {broker_name}: {e}")

    def recv_all(self, length):
        """Helper function to receive the exact amount of data."""
        data = b''
        while len(data) < length:
            packet = self.connection.recv(length - len(data))
            if not packet:
                raise ConnectionError("Socket connection broken")
            data += packet
        return data

# Example usage
if __name__ == "__main__":
    SERVER_IP = "127.0.0.1"
    SERVER_PORT = 8080
    KEY = "a8eecf33-c18c-4d78-bf22-3770406e7768"

    client = Client(SERVER_IP, SERVER_PORT, KEY)

    try:
        # Example: Send a message
        response = client.send_push_message("test_broker", b"Hello, World!")
        print(f"Push response: {response}")

        # Example: Fetch messages
        offset = 0
        while True:
            result = client.fetch_messages("test_broker", offset)
            if result is None:
                break
            offset, message = result
            print(f"Received message at offset {offset}: {message}")
    finally:
        client.close()
