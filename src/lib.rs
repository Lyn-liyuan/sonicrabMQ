use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::error::Error;

const PUSH_COMMAND: &[u8] = b"PUSH";
const PULL_COMMAND: &[u8] = b"PULL";

pub struct Client {
    server_ip: String,
    server_port: u16,
    key: Vec<u8>,
    connection: Mutex<Option<TcpStream>>,
}

impl Client {
    /// Creates a new client instance
    pub fn new(server_ip: &str, server_port: u16, key: &str) -> Self {
        Self {
            server_ip: server_ip.to_string(),
            server_port,
            key: key.as_bytes().to_vec(),
            connection: Mutex::new(None),
        }
    }

    /// Connects to the server
    fn connect(&self) -> Result<(), Box<dyn Error>> {
        let mut connection = self.connection.lock().unwrap();
        if connection.is_none() {
            let stream = TcpStream::connect((self.server_ip.as_str(), self.server_port))?;
            *connection = Some(stream);
        }
        Ok(())
    }

    /// Sends a message to the queue
    pub fn send_push_message(&self, broker_name: &str, payload: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        self.connect()?;
        let mut connection = self.connection.lock().unwrap();
        let stream = connection.as_mut().unwrap();

        let broker_name_bytes = broker_name.as_bytes();
        let message = self.build_message(PUSH_COMMAND, broker_name_bytes, payload, None)?;

        // Send message length and message body
        stream.write_all(&(message.len() as u32).to_be_bytes())?;
        stream.write_all(&message)?;

        // Receive response length
        let mut response_length_bytes = [0u8; 4];
        stream.read_exact(&mut response_length_bytes)?;
        let response_length = u32::from_be_bytes(response_length_bytes);

        // Receive response content
        let mut response = vec![0u8; response_length as usize];
        stream.read_exact(&mut response)?;
        Ok(response)
    }

    /// Fetches messages from the queue
    pub fn fetch_messages(&self, broker_name: &str, offset: u64) -> Result<Option<(u64, Vec<u8>)>, Box<dyn Error>> {
        self.connect()?;
        let mut connection = self.connection.lock().unwrap();
        let stream = connection.as_mut().unwrap();

        let broker_name_bytes = broker_name.as_bytes();
        let message = self.build_message(PULL_COMMAND, broker_name_bytes, &[], Some(offset))?;

        // Send request
        stream.write_all(&(message.len() as u32).to_be_bytes())?;
        stream.write_all(&message)?;

        // Read response length
        let mut response_length_bytes = [0u8; 4];
        stream.read_exact(&mut response_length_bytes)?;
        let response_length = u32::from_be_bytes(response_length_bytes);

        if response_length == 0 {
            return Ok(None); // No more messages
        }

        // Read new offset
        let mut new_offset_bytes = [0u8; 8];
        stream.read_exact(&mut new_offset_bytes)?;
        let new_offset = u64::from_be_bytes(new_offset_bytes);

        // Read message body
        let mut message_data = vec![0u8; response_length as usize];
        stream.read_exact(&mut message_data)?;

        Ok(Some((new_offset, message_data)))
    }

    /// Constructs a message
    fn build_message(
        &self,
        command: &[u8],
        broker_name: &[u8],
        payload: &[u8],
        offset: Option<u64>,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut message = Vec::new();
        message.extend_from_slice(&(self.key.len() as u16).to_be_bytes());
        message.extend_from_slice(&self.key);
        message.extend_from_slice(&(command.len() as u16).to_be_bytes());
        message.extend_from_slice(command);
        message.extend_from_slice(&(broker_name.len() as u16).to_be_bytes());
        message.extend_from_slice(broker_name);

        if let Some(offset_value) = offset {
            message.extend_from_slice(&offset_value.to_be_bytes());
        }

        message.extend_from_slice(payload);
        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client() {
        let client = Client::new("127.0.0.1", 8080, "example_key");

        // Test sending a message
        let response = client.send_push_message("test_broker", b"Hello, World!");
        assert!(response.is_ok());

        // Test fetching a message
        let result = client.fetch_messages("test_broker", 0);
        assert!(result.is_ok());
    }
}