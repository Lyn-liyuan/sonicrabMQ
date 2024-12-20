use std::fs;
use std::path::PathBuf;
use std::error::Error;

pub async fn delete_old_files(directory: &str, max_files: usize) -> Result<(), Box<dyn Error>> {
    // 递归遍历目录及其子目录
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if entry.path().is_dir() {
            clean_directory(entry.path(), max_files)?;
        }
    }

    Ok(())
}

fn clean_directory(dir: PathBuf, max_files: usize) -> Result<(), Box<dyn Error>> {
    // 获取子目录中的所有文件，并过滤出以.index或.data结尾的文件
    let mut files: Vec<PathBuf> = vec![];

    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
            if ext == "index" || ext == "data" {
                files.push(path);
            }
        }
    }

    // 按照文件名（时间戳部分）排序
    files.sort_by_key(|path| path.file_name().unwrap().to_owned());

    // 如果文件数量超过max_files，删除最老的文件
    if files.len() > max_files {
        let files_to_delete = &files[..files.len() - max_files];  // 保留最新的max_files个文件
        for file in files_to_delete {
            fs::remove_file(file)?;
            println!("Deleted: {:?}", file);
        }
    }

    Ok(())
}