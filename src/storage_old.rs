use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::io::AsFd;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use memmap2::MmapMut;
use nix::sys::sendfile::sendfile;
use std::collections::BTreeMap;

const INDEX_INITIAL_SIZE: usize = 1024 * 16; // Initial space for index file

pub struct DataStore {
    files: Arc<RwLock<BTreeMap<u64, File>>>,
    index_map: Arc<RwLock<BTreeMap<u64, MmapMut>>>,
    current_file_offset: Arc<RwLock<u64>>,
    file_size_limit: u64,
}

impl DataStore {
    pub fn new(file_size_limit: u64, dir: &Path) -> io::Result<Self> {
        let mut files = BTreeMap::new();
        let mut index_map = BTreeMap::new();

        // Load existing data and index files from the directory
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                let filename = path.file_stem().unwrap().to_str().unwrap();
                let offset: u64 = filename.parse().unwrap();

                if path.extension().unwrap() == "dat" {
                    let file = OpenOptions::new().read(true).append(true).open(&path)?;
                    files.insert(offset, file);
                } else if path.extension().unwrap() == "index" {
                    let file = OpenOptions::new().read(true).write(true).open(&path)?;
                    let index_data = unsafe { MmapMut::map_mut(&file)? };
                    index_map.insert(offset, index_data);
                }
            }
        }

        // If no files exist, create initial data and index files
        if files.is_empty() {
            let initial_offset = 0;
            let (data_offset, data_file) = Self::create_file(dir, initial_offset).await?;
            files.insert(data_offset, data_file);

            let (index_offset, _, index_mmap) = Self::create_index_file(dir, initial_offset).await?;
            index_map.insert(index_offset, index_mmap);
        }

        let current_file_offset = files.keys().max().cloned().unwrap_or(0);

        Ok(DataStore {
            files: Arc::new(RwLock::new(files)),
            index_map: Arc::new(RwLock::new(index_map)),
            current_file_offset: Arc::new(RwLock::new(current_file_offset)),
            file_size_limit,
        })
    }

    async fn create_file(dir: &Path, offset: u64) -> io::Result<(u64, File)> {
        let filename = dir.join(format!("{:020}.dat", offset));
        let file = OpenOptions::new().create(true).read(true).append(true).open(&filename)?;
        Ok((offset, file))
    }

    async fn create_index_file(dir: &Path, offset: u64) -> io::Result<(u64, File, MmapMut)> {
        let filename = dir.join(format!("{:020}.index", offset));
        let file = OpenOptions::new().create(true).read(true).write(true).open(&filename)?;
        file.set_len(INDEX_INITIAL_SIZE as u64)?; // Pre-allocate space for the index file
        let index_mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok((offset, file, index_mmap))
    }

    pub async fn append_data(&self, data: &[u8]) -> io::Result<()> {
        let mut files = self.files.write().await;
        let mut index_map = self.index_map.write().await;
        let mut current_offset_guard = self.current_file_offset.write().await;

        let current_offset = *current_offset_guard;
        
        let file = match files.get_mut(&current_offset) {
            Some(file) => {
                let metadata = file.metadata()?;
                if metadata.len() + data.len() as u64 > self.file_size_limit {
                    // Create a new file and corresponding index file
                    *current_offset_guard += 1;
                    let new_offset = *current_offset_guard;
                    let (new_file_offset, new_file) = Self::create_file(Path::new("."), new_offset).await?;
                    files.insert(new_file_offset, new_file);

                    let (_, _, new_index_mmap) = Self::create_index_file(Path::new("."), new_offset).await?;
                    index_map.insert(new_offset, new_index_mmap);

                    files.get_mut(&new_offset).unwrap()
                } else {
                    file
                }
            }
            None => {
                // Create the initial file and index file
                let (offset, file) = Self::create_file(Path::new("."), current_offset).await?;
                files.insert(offset, file);

                let (_, _, index_mmap) = Self::create_index_file(Path::new("."), offset).await?;
                index_map.insert(offset, index_mmap);

                files.get_mut(&current_offset).unwrap()
            }
        };

        let start_offset = file.metadata()?.len();
        file.write_all(data)?;

        // Update index using mmap
        let index_mmap = index_map.get_mut(&current_offset).unwrap();
        let position = index_mmap.len();

        if position + 16 > index_mmap.len() {
            // Increase the file size if necessary to avoid frequent resizing
            let new_size = (position + 16 + INDEX_INITIAL_SIZE - 1) / INDEX_INITIAL_SIZE * INDEX_INITIAL_SIZE;
            let index_file_path = Path::new(".").join(format!("{:020}.index", current_offset));
            let index_file = OpenOptions::new().write(true).open(&index_file_path)?;
            index_file.set_len(new_size as u64)?;

            // Remap the file
            let new_index_mmap = unsafe { MmapMut::map_mut(&index_file)? };
            index_map.insert(current_offset, new_index_mmap);
        }

        let index_mmap = index_map.get_mut(&current_offset).unwrap();
        (&mut index_mmap[position..position+8]).copy_from_slice(&start_offset.to_be_bytes());
        (&mut index_mmap[position+8..position+16]).copy_from_slice(&(start_offset + data.len() as u64).to_be_bytes());

        Ok(())
    }

    async fn read_data(&self, offset: u64) -> io::Result<(u64, u64, Option<u64>)> {
        let index_map = self.index_map.read().await;

        let (&index_start_offset, index_data) = index_map.range(..=offset).next_back().ok_or(io::Error::new(io::ErrorKind::NotFound, "Index not found"))?;

        let record_length = 16; // 每条记录存储两个u64，每个占8字节，共16字节
        let relative_offset = offset - index_start_offset;
        let record_position = (relative_offset / record_length as u64) as usize * record_length;

        let record = &index_data[record_position..record_position + record_length];
        let start_offset = u64::from_be_bytes(record[0..8].try_into().unwrap());
        //let end_offset = u64::from_be_bytes(record[8..16].try_into().unwrap());

        let next_offset = if record_position + record_length < index_data.len() {
            Some(offset + 1)
        } else {
            None
        };

        Ok((index_start_offset, start_offset, next_offset))
    }

    pub async fn sendfile<S>(&self, offset: u64, sock_fd: S) -> io::Result<Option<u64>> 
    where S: AsFd
    {
        let files = self.files.read().await;

        let (index_start_offset, start_offset, next_offset) = self.read_data(offset).await?;
        let file = files.get(&index_start_offset).unwrap();
        let file_fd = file.as_fd();

        let len = usize::try_from(file.metadata()?.len() - start_offset).unwrap_or(usize::MAX);

        sendfile(sock_fd, file_fd, Some(&mut (start_offset as i64)), len).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(next_offset)
    }
}