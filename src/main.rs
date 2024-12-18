use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use dashmap::DashMap;
use std::fs;
use serde::{Deserialize, Serialize};

use std::path::PathBuf;
use std::io::Cursor;
use std::io::{self,Read, Write};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use std::os::unix::io::AsFd;
mod storage;
use crate::storage::DataStorage;
mod config;
use crate::config::Config;





#[derive(Serialize, Deserialize, Debug, Clone)]
struct Message {
    id: usize,
    payload: Vec<u8>,
}

struct Broker {
    store:DataStorage
}

impl Broker {
    
    async fn new(name: String,config:&Config) -> Self {
        let broker_path = config.server.path.clone() + "/" + name.as_str();
        if create_directory_if_not_exists(broker_path.as_str()).is_err() {
            println!("crate breaker {} path failed!", name)
        }
        let file_dir = PathBuf::from(broker_path);
        let manager = DataStorage::new(file_dir,&config.storage).await.unwrap();
        
        Broker {
           store: manager,
        }
    }

    // 接收消息并保存到文件中，同时记录消息ID与文件偏移量
    async fn receive_message(&mut self, payload: Vec<u8>) -> io::Result<()>{
        self.store.append_data(&payload).await?;
        Ok(())       
    }

    // 根据客户端提供的最后一条消息ID来获取文件偏移量，并用 sendfile 发送消息给客户端
    async fn send_messages_since(&mut self, last_id: usize, stream: &mut TcpStream) -> io::Result<()>{
        self.store.sendfile(last_id as u64, stream.as_fd()).await?;
        let end = (0u32).to_be_bytes();
        stream.write_all(&end).await?;
        Ok(())
    }
}

async fn handle_client(
    mut stream: TcpStream,
    brokers: Arc<DashMap<String, Arc<RwLock<Broker>>>>,
    config:Config
) -> io::Result<()>{
    loop {
        let mut len_buf = [0; 4];
        if AsyncReadExt::read_exact(&mut stream, &mut len_buf)
            .await
            .is_err()
        {
            break;
        }
        let message_len =
            ReadBytesExt::read_u32::<BigEndian>(&mut Cursor::new(len_buf)).unwrap() as usize;
        let mut buffer = vec![0; message_len];
        if AsyncReadExt::read_exact(&mut stream, &mut buffer)
            .await
            .is_err()
        {
            break;
        }

        let mut cursor = Cursor::new(buffer);
        let command_len = ReadBytesExt::read_u16::<BigEndian>(&mut cursor).unwrap() as usize;
        let mut command_buf = vec![0; command_len];
        std::io::Read::read_exact(&mut cursor, &mut command_buf).unwrap();
        let command = String::from_utf8(command_buf).unwrap();

        if command == "PUSH" {
            let broker_len = ReadBytesExt::read_u16::<BigEndian>(&mut cursor).unwrap() as usize;
            let mut broker_buf = vec![0; broker_len];
            Read::read_exact(&mut cursor, &mut broker_buf).unwrap();
            let broker_name = String::from_utf8(broker_buf).unwrap();
            let position = cursor.position() as usize;
            let payload = cursor.into_inner()[position..].to_vec();
            let broker = get_broker(&brokers, broker_name,&config).await;

            broker.write().await.receive_message(payload).await?;
            let mut response = Vec::new();
            WriteBytesExt::write_u32::<BigEndian>(&mut response, 2).unwrap();
            Write::write_all(&mut response, b"OK").unwrap();
            let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, &response).await;
        } else if command == "PULL" {
            let broker_len = ReadBytesExt::read_u16::<BigEndian>(&mut cursor).unwrap() as usize;
            let mut broker_buf = vec![0; broker_len];
            Read::read_exact(&mut cursor, &mut broker_buf).unwrap();
            let broker_name = String::from_utf8(broker_buf).unwrap();
            
            let offset = ReadBytesExt::read_u64::<BigEndian>(&mut cursor).unwrap();

            let broker = get_broker(&brokers, broker_name,&config).await;
            broker
                .write()
                .await
                .send_messages_since(offset as usize, &mut stream)
                .await?;
        }
    }
    Ok(())
}

async fn get_broker(brokers: &Arc<DashMap<String, Arc<RwLock<Broker>>>>, broker_name: String, config:&Config) -> Arc<RwLock<Broker>> {
        
        if brokers.contains_key(&broker_name) {
            brokers.get(&broker_name).unwrap().clone()
        } else {
            let new_broker = Arc::new(RwLock::new(Broker::new(broker_name.clone(),config).await));
            brokers.insert(broker_name, new_broker.clone());
            new_broker
        }
        
}

fn create_directory_if_not_exists(path: &str) -> std::io::Result<()> {
    if !std::fs::metadata(path).map(|m| m.is_dir()).unwrap_or(false) {
        std::fs::create_dir_all(path)?;
        println!("Directory created: {}", path);
    } else {
        println!("Directory already exists: {}", path);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let config_content = fs::read_to_string("config.toml")?;
    
    let config: Config = toml::from_str(&config_content)?;

    create_directory_if_not_exists(&config.server.path)?;
    let brokers = Arc::new(DashMap::new());
    let address = format!("{}:{}",config.server.address,config.server.port);
    let listener = TcpListener::bind(address).await?;

    println!("Broker server is running on 0.0.0.0:8080");

    loop {
        let (stream, _) = listener.accept().await?;
        let brokers = brokers.clone();
        let config = config.clone();
        tokio::spawn(async move {
            handle_client(stream, brokers,config).await.unwrap();
        });
    }
}
