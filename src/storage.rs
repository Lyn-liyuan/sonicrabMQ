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

const MAX_FILE_SIZE: u64 = 1024 * 1024 * 100; // 1 GB
const INDEX_ENTRY_SIZE: usize = 16;
const INITIAL_INDEX_SIZE: usize = 1024 * INDEX_ENTRY_SIZE; // Initial index file size
const INDEX_EXPANSION_SIZE: usize = 512 * INDEX_ENTRY_SIZE; // Index expansion size
const PULL_MAX_LIMIT: usize = 1024 * 1024 * 100; // Example threshold for pull limit

type Offset = AtomicU64;

struct FileEntry {
    base_offset: u64,
    data_file: File,
    data: MmapMut,
}

pub struct DataStorage {
    data_dir: PathBuf,
    base_offset: Offset,
    position_offset: Offset,
    index_len: Offset,
    data_len: Offset,
    data_file: Option<RwLock<File>>,
    index_file: Option<RwLock<File>>,
    index_map: Option<RwLock<MmapMut>>,
    files: RwLock<Vec<FileEntry>>,
}

impl DataStorage {
    pub async fn new(data_dir: PathBuf) -> io::Result<Self> {
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

    async fn read_index(&self, postion: usize) -> io::Result<u64> {
        if let Some(index_map_lock) = &self.index_map {
            let index_map = index_map_lock.read().await;
            Ok((&index_map[postion..postion + 8usize]).read_u64::<BigEndian>()?)
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Appropriate index map read failed",
            ))
        }
    }

    async fn initialize_files(&mut self) -> io::Result<()> {
        let mut offsets = vec![];

        for entry in std::fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();
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

        if offsets.is_empty() {
            self.create_new_files(0).await?;
        } else {
            offsets.sort();
            if let Some(&last_offset) = offsets.last() {
                self.base_offset.store(last_offset, Ordering::SeqCst);
                for file_name in offsets {
                    if file_name == last_offset {
                        continue;
                    } else {
                        let data_file = self.open_data_file(file_name).await?;
                        let (_, map) = self.open_index_file(file_name).await?;
                        self.files.write().await.push(FileEntry {
                            base_offset: file_name,
                            data_file: data_file,
                            data: map,
                        });
                    }
                }
                self.create_new_files(last_offset).await?;

                let index_len = self.get_index_len().await?;
                self.index_len.swap(index_len, Ordering::SeqCst);
                for index in (0..index_len).step_by(INDEX_ENTRY_SIZE) {
                    let start = self.read_index(index as usize).await?;
                    let end = self
                        .read_index(index as usize + INDEX_ENTRY_SIZE / 2)
                        .await?;
                    if start == 0 && end == 0 {
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
        let data_file = self.open_data_file(offset).await?;
        let (index_file, map) = self.create_index_file(offset).await?;
        let data_len = data_file.metadata()?.len();
        self.data_len.swap(data_len, Ordering::SeqCst);
        let index_len = index_file.metadata()?.len();
        self.index_len.swap(index_len, Ordering::SeqCst);

        self.data_file = Some(RwLock::new(data_file));
        self.index_file = Some(RwLock::new(index_file));
        self.index_map = Some(RwLock::new(map));

        let base_offset = self.base_offset.swap(offset, Ordering::SeqCst);
        let data_file = self.open_data_file(base_offset).await?;
        let (_, map) = self.open_index_file(base_offset).await?;
        self.files.write().await.push(FileEntry {
            base_offset: base_offset,
            data_file: data_file,
            data: map,
        });
        Ok(())
    }

    async fn open_data_file(&self, offset: u64) -> io::Result<File> {
        let path = self.data_dir.join(format!("{:012}.data", offset));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(file)
    }

    async fn open_index_file(&self, offset: u64) -> io::Result<(File, MmapMut)> {
        let path = self.data_dir.join(format!("{:012}.index", offset));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false) // Do not create a new file; only open an existing one
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

    pub async fn append_data(&mut self, data: &[u8]) -> io::Result<()> {
        if self.data_len.load(Ordering::SeqCst) + data.len() as u64 > MAX_FILE_SIZE {
            let position = self.position_offset.load(Ordering::SeqCst);
            self.create_new_files(position).await?;
        }
        let base_offset = self.base_offset.load(Ordering::SeqCst);
        let position = self.position_offset.load(Ordering::SeqCst);
        let new_size = (position + 2 - base_offset) * (INDEX_ENTRY_SIZE as u64);
        let old_size = self.get_index_len().await?;
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
            data_file.write_all(&header)?;
            data_file.write_all(data)?;
            let end = start + header.len() as u64 + data.len() as u64;
            self.data_len
                .fetch_add(header.len() as u64 + data.len() as u64, Ordering::SeqCst);
            if let Some(index_map_lock) = &self.index_map {
                let mut index_map = index_map_lock.write().await;
                let entry_start = (position - base_offset) as usize * INDEX_ENTRY_SIZE;
                (&mut index_map[entry_start..entry_start + 8usize])
                    .copy_from_slice(&start.to_be_bytes());
                (&mut index_map[entry_start + 8usize..entry_start + 16usize])
                    .copy_from_slice(&end.to_be_bytes());
                (&mut index_map[entry_start + 16usize..entry_start + 24usize])
                    .copy_from_slice(&0u64.to_be_bytes());
                (&mut index_map[entry_start + 24usize..entry_start + 32usize])
                    .copy_from_slice(&0u64.to_be_bytes());
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

    pub async fn sendfile<S>(&self, offset: u64, sock_fd: S) -> io::Result<usize>
    where
        S: AsFd + Clone,
    {
        // Find the correct index file by range
        let base_offset = self.base_offset.load(Ordering::SeqCst);
        let position = self.position_offset.load(Ordering::SeqCst);

        if offset >= base_offset && position > offset {
            let index_position = (offset - base_offset) as usize * INDEX_ENTRY_SIZE;
            let start = self.read_index(index_position).await?;
            let end = self.read_index(index_position + INDEX_ENTRY_SIZE).await?;
            let len = self.data_len.load(Ordering::SeqCst);
            let size = if len - start > PULL_MAX_LIMIT as u64 {
                (end - start) as usize
            } else {
                (len - start) as usize
            };

            if let Some(data_file_locked) = &self.data_file {
                let data_file = data_file_locked.read().await;
                let in_fd = data_file.as_fd();
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
            if len > 0 {
                let mut pre = 0;
                for i in 0..len {
                    let base = guard[i].base_offset;
                    if offset < base {
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
            if let Some(entry) = selected_file {
                let index_position = (offset - entry.base_offset) as usize * INDEX_ENTRY_SIZE;
                let start =
                    (&entry.data[index_position..index_position + 8]).read_u64::<BigEndian>()?;
                let end = (&entry.data[index_position + 8..index_position + 16])
                    .read_u64::<BigEndian>()?;
                let len = entry.data_file.metadata()?.len();
                let size = if len - start > PULL_MAX_LIMIT as u64 {
                    (end - start) as usize
                } else {
                    (len - start) as usize
                };
                let in_fd = entry.data_file.as_fd();
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
