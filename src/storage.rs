use std::fs::{File, OpenOptions};
use std::io::{self, Write};

use byteorder::{BigEndian, ReadBytesExt};
use memmap2::MmapMut;
use nix::errno::Errno;
use nix::sys::sendfile::sendfile;

use std::os::unix::io::AsFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;
use std::os::unix::prelude::BorrowedFd;
use crate::config::{Storage,parse_size};


const INDEX_ENTRY_SIZE: usize = 12;
const INITIAL_INDEX_SIZE: usize = 1024 * INDEX_ENTRY_SIZE; // Initial index file size
const INDEX_EXPANSION_SIZE: usize = 512 * INDEX_ENTRY_SIZE; // Index expansion size


type Offset = AtomicU64;
#[derive(Clone)]
struct IndexEntry {
    start:u64,
    size:u32,
}

struct FileEntry {
    base_offset: u64, //历史索引文件的基础偏移
    data_file: File, // 数据文件
    data: MmapMut, // 索引内存映射
}

pub struct DataStorage {
    data_dir: PathBuf,
    base_offset: Offset, // 当前索引文件的基础偏移
    position_offset: Offset, // 当前索引文件的偏移位置
    index_len: Offset, //索引文件长度
    data_len: Offset, //数据文件长度
    data_file: Option<RwLock<File>>, //当前数据文件
    index_file: Option<RwLock<File>>, //当前索引文件
    index_map: Option<RwLock<MmapMut>>, //当前索引文件的内存映射
    files: RwLock<Vec<FileEntry>>, //历史文件项
    max_file_size: usize,
    pull_max_limit: usize,
    cache_limit: usize,
}

impl DataStorage {
    pub async fn new(data_dir: PathBuf,config:&Storage) -> io::Result<Self> {
        
        let mut storage = Self {
            data_dir,
            base_offset: AtomicU64::new(0),
            position_offset: AtomicU64::new(0),
            index_len: AtomicU64::new(0),
            data_len: AtomicU64::new(0),
            data_file: None,
            index_file: None,
            index_map: None,
            files: Vec::new().into(),
            max_file_size: parse_size(config.max_file_size.as_str()).unwrap_or_else(|_|1024 * 1024 * 100 as usize),
            pull_max_limit: parse_size(config.pull_max_limit.as_str()).unwrap_or_else(|_|1024 * 1024 * 50 as usize),
            cache_limit: config.cache_limit,
        };
        storage.initialize_files().await?;
        Ok(storage)
    }

    async fn get_index_len(&self) -> io::Result<u64> {
        if let Some(index_file_lock) = &self.index_file {
            let index_file = index_file_lock.read().await; // 获取读锁
            let metadata = index_file.metadata()?; // 获取元数据
            Ok(metadata.len()) // 返回文件长度
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Appropriate index file not set",
            ))
        }
    }

    async fn set_index_len(&self, new_size: u64) -> io::Result<()> {
        if let Some(index_file_lock) = &self.index_file {
            let index_file = index_file_lock.write().await; // 获取读锁
            index_file.set_len(new_size)?;
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Appropriate index file not set",
            ))
        }
    }

    async fn get_data_len(&self) -> io::Result<u64> {
        if let Some(data_file_lock) = &self.data_file {
            let data_file = data_file_lock.read().await; // 获取读锁
            let metadata = data_file.metadata()?; // 获取元数据
            Ok(metadata.len()) // 返回文件长度
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Appropriate data file not set",
            ))
        }
    }

    async fn read_index(&self, postion: usize) -> io::Result<IndexEntry> {
        if let Some(index_map_lock) = &self.index_map {
            let index_map = index_map_lock.read().await;
            let start = (&index_map[postion..postion + 8usize]).read_u64::<BigEndian>()?;
            let size = (&index_map[postion+8usize..postion + 12usize]).read_u32::<BigEndian>()?;
            Ok(IndexEntry{
                start,
                size
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Appropriate index map read failed",
            ))
        }
    }

    // 从目录中恢复 DataStorage 的相关字段 
    async fn initialize_files(&mut self) -> io::Result<()> {
        let mut offsets = vec![];

        for entry in std::fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();
            // 目录中读取文件名作为历史文件的 base_offset
            if path.extension().and_then(|s| s.to_str()) == Some("data") {
                if let Some(offset) = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    offsets.push(offset);
                }
            }
        }
        // 判断目录是否为空
        if offsets.is_empty() {
            // 数据目录为空，创建新的数据和索引文件
            self.create_new_files(0).await?;
        } else {
            // 排序文件名
            offsets.sort();
            if let Some(&last_offset) = offsets.last() {
                // 设置当前基础偏移为最新文件的基础偏移
                self.base_offset.store(last_offset, Ordering::SeqCst);
                for file_name in offsets.iter().rev() {
                    if *file_name == last_offset {
                        // 当前文件后续进行操作
                        continue;
                    } else {
                        // 历史文件打开并装载到历史文件列表中
                        let mut files = self.files.write().await;
                        if files.len() + 1 > self.cache_limit {
                            break;
                        }
                        let data_file = self.open_data_file(*file_name,true).await?;
                        let (_, map) = self.open_index_file(*file_name).await?;
                        
                        files.push(FileEntry {
                            base_offset: *file_name,
                            data_file: data_file,
                            data: map,
                            
                        });
                    }
                }
                // 创建当前文件
                self.create_new_files(last_offset).await?;

                let index_len = self.get_index_len().await?;
                self.index_len.swap(index_len, Ordering::SeqCst);
                // 从索引文件读取当前偏移位置 position_offset
                for index in (0..index_len).step_by(INDEX_ENTRY_SIZE) {
                    let index_entry = self.read_index(index as usize).await?;
                    
                    if index_entry.start == 0 && index_entry.size == 0 {
                        if index > 0 {
                            self.position_offset.swap(
                                index / (INDEX_ENTRY_SIZE as u64) + last_offset,
                                Ordering::SeqCst,
                            );
                        } else {
                            self.position_offset.swap(last_offset, Ordering::SeqCst);
                        }
                        break;
                    }
                }
                self.data_len
                    .swap(self.get_data_len().await?, Ordering::SeqCst);
            }
        }

        Ok(())
    }

    async fn create_new_files(&mut self, offset: u64) -> io::Result<()> {
        // 创建数据文件
        let data_file = self.open_data_file(offset,false).await?;
        // 采用 offset 作为文件名创建索引文件
        let (index_file, map) = self.create_index_file(offset).await?;
        // 设置 storage 各个字段
        let data_len = data_file.metadata()?.len();
        self.data_len.swap(data_len, Ordering::SeqCst);
        let index_len = index_file.metadata()?.len();
        self.index_len.swap(index_len, Ordering::SeqCst);

        self.data_file = Some(RwLock::new(data_file));
        self.index_file = Some(RwLock::new(index_file));
        self.index_map = Some(RwLock::new(map));
        Ok(())
    }

    async fn open_data_file(&self, offset: u64, readonly: bool) -> io::Result<File> {
        let path = self.data_dir.join(format!("{:012}.data", offset));
        let file = if readonly { OpenOptions::new()
            .read(true).open(&path)?
        } else {
            OpenOptions::new()
            .read(true)
            .write(true)
            .create(true) // Do not create a new file; only open an existing one
            .open(&path)?
        };
        Ok(file)
    }

    async fn open_index_file(&self, offset: u64) -> io::Result<(File, MmapMut)> {
        let path = self.data_dir.join(format!("{:012}.index", offset));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true) // Do not create a new file; only open an existing one
            .open(&path)?;

        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok((file, mmap))
    }

    async fn create_index_file(&self, offset: u64) -> io::Result<(File, MmapMut)> {
        let path = self.data_dir.join(format!("{:012}.index", offset));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        file.set_len(INITIAL_INDEX_SIZE as u64)?; // Preallocate initial space
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok((file, mmap))
    }

    async fn expand_index_file(&mut self, new_size: u64) -> io::Result<()> {
        self.set_index_len(new_size).await?;
        if let Some(index_file_lock) = &self.index_file {
            let file = index_file_lock.read().await;
            let mmap = unsafe { MmapMut::map_mut(&*file)? };
            self.index_map = Some(RwLock::new(mmap));
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Appropriate data file not set",
            ))
        }
    }
    // 将消息写入文件中并建立索引
    pub async fn append_data(&mut self, data: &[u8]) -> io::Result<()> {
        // 超过阈值创立新文件
        if self.data_len.load(Ordering::SeqCst) + data.len() as u64 > self.max_file_size as u64 {
            let position = self.position_offset.load(Ordering::SeqCst);
            self.create_new_files(position).await?;
            // 因为创建了新文件，把当前文件重新只读打开放入历史文件列表
            let mut files = self.files.write().await;
            if files.len() + 1 > self.cache_limit {
                files.remove(0);
            }
            let base_offset = self.base_offset.swap(position, Ordering::SeqCst);
            let data_file = self.open_data_file(base_offset,true).await?;
            let (_, map) = self.open_index_file(base_offset).await?;
            files.push(FileEntry {
                base_offset: base_offset,
                data_file: data_file,
                data: map,
            });
            
        }
        
        let base_offset = self.base_offset.load(Ordering::SeqCst);
        let position = self.position_offset.load(Ordering::SeqCst);
        let new_size = (position + 2 - base_offset) * (INDEX_ENTRY_SIZE as u64);
        let old_size = self.get_index_len().await?;
        // 新增的索引项超过索引文件的长度，需要扩展索引文件
        if new_size > old_size {
            self.expand_index_file(old_size + INDEX_EXPANSION_SIZE as u64)
                .await?;
            self.index_len
                .swap(INDEX_EXPANSION_SIZE as u64, Ordering::SeqCst);
        }

        if let Some(data_file_lock) = &self.data_file {
            let start = self.get_data_len().await?;
            let mut data_file = data_file_lock.write().await; // 获取读锁
            let mut header = [0u8; 12];
            let data_len = data.len() as u32;
            header[0..4].copy_from_slice(&data_len.to_be_bytes());
            header[4..12].copy_from_slice(&position.to_be_bytes());
            // 写入记录头和数据
            data_file.write_all(&header)?;
            data_file.write_all(data)?;
            let end = header.len() as u32 + data.len() as u32;
            self.data_len
                .fetch_add(header.len() as u64 + data.len() as u64, Ordering::SeqCst);
            if let Some(index_map_lock) = &self.index_map {
                // 将记录位置写入索引
                let mut index_map = index_map_lock.write().await;
                let entry_start = (position - base_offset) as usize * INDEX_ENTRY_SIZE;
                (&mut index_map[entry_start..entry_start + 8usize])
                    .copy_from_slice(&start.to_be_bytes());
                (&mut index_map[entry_start + 8usize..entry_start + 12usize])
                    .copy_from_slice(&end.to_be_bytes());
                // 在最新索引项后面加入0，以便重启的时候设置position_offset
                (&mut index_map[entry_start + 12usize..entry_start + 20usize])
                    .copy_from_slice(&0u64.to_be_bytes());
                (&mut index_map[entry_start + 20usize..entry_start + 24usize])
                    .copy_from_slice(&0u32.to_be_bytes());
                self.position_offset.fetch_add(1, Ordering::SeqCst);
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "Appropriate index file not set",
                ))
            }
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Appropriate data file not set",
            ))
        }
    }
    
    // 在当前或者历史文件定位数据并通过sendfile发送
    pub async fn sendfile<S>(&self, since_offset: u64, sock_fd: S) -> io::Result<usize>
    where
        S: AsFd + Clone,
    {
        // Find the correct index file by range
        let base_offset = self.base_offset.load(Ordering::SeqCst);
        let position = self.position_offset.load(Ordering::SeqCst);
        let offset = if since_offset == 0 && position>0 {
            position-1
        } else {
            since_offset
        };
        // 在当前文件中
        if offset >= base_offset && position > offset {
            let index_position = (offset - base_offset) as usize * INDEX_ENTRY_SIZE;
            let index_entry = self.read_index(index_position).await?;
           
            let len = self.data_len.load(Ordering::SeqCst);
            // 不大于阈值的情况下，尽量多的返回记录
            let size = if len - index_entry.start > self.pull_max_limit as u64 {
                index_entry.size as usize
            } else {
                (len - index_entry.start) as usize
            };

            if let Some(data_file_locked) = &self.data_file {
                let data_file = data_file_locked.read().await;
                let in_fd = data_file.as_fd();
                // 发送当前文件的数据
                let _size = call_sendfile(sock_fd,in_fd, index_entry.start, size);
                Ok(size - _size)
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "Appropriate data file not set",
                ));
            }
        } else {
            let mut selected_file = None;
            let guard = self.files.read().await;
            let len = guard.len();
            // 在历史文件中定位索引项
            if len > 0 {
                let mut pre = 0;
                for i in 0..len {
                    let base = guard[i].base_offset;
                    if offset < base && offset > guard[pre].base_offset {
                        selected_file = Some(&guard[pre]);
                        break;
                    }
                    pre = i
                }
                if selected_file.is_none() {
                    if offset < base_offset && offset >= guard[len - 1].base_offset {
                        selected_file = Some(&guard[len - 1]);
                    }
                }
            }
            // 找到匹配的索引文件获取索引项并根据索引项发送数据
            if let Some(entry) = selected_file {
                let index_position = (offset - entry.base_offset) as usize * INDEX_ENTRY_SIZE;
                let start =
                    (&entry.data[index_position..index_position + 8]).read_u64::<BigEndian>()?;
                let end = start + (&entry.data[index_position + 8..index_position + 12])
                    .read_u32::<BigEndian>()? as u64;
                let len = entry.data_file.metadata()?.len();
                let size = if len - start > self.pull_max_limit as u64 {
                    (end - start) as usize
                } else {
                    (len - start) as usize
                };
                let in_fd = entry.data_file.as_fd();
                let _size = call_sendfile(sock_fd,in_fd, start, size);
                Ok(size - _size)
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "index file not match",
                ));
            }
        }
    }
}
// 调用 linux 函数 sendfile 零拷贝发送数据
fn call_sendfile<S>(sock_fd: S, in_fd: BorrowedFd<'_>, start: u64, size: usize) -> usize where S: AsFd + Clone {
    let mut _size = size;
    let mut _start = start as i64;
    loop {
        match sendfile(sock_fd.clone(), in_fd, Some(&mut _start), _size) {
            Ok(sent_count) => {
                if sent_count == 0 {
                    break;
                }
                _size = _size - sent_count
            }
            Err(e) => {
                if e == Errno::EAGAIN {
                    continue;
                }
            }
        };
    }
    _size
}
