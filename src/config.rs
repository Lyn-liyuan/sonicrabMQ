use serde::Deserialize;
use regex::Regex;

#[derive(Debug, Deserialize,Clone)]
pub struct Server {
    pub address: String,
    pub port: u16,
    pub path: String,
    pub broker_limit: u16,
    pub authorization: String
}

#[derive(Debug, Deserialize,Clone)]
pub struct Storage {
    pub max_file_size: String,
    pub pull_max_limit: String,
    pub cache_limit: usize,
    
}

#[derive(Debug, Deserialize,Clone)]
pub struct Config {
    pub server: Server,
    pub storage: Storage,
}

pub fn parse_size(size_str: &str) -> Result<usize, &'static str> {
    let re = Regex::new(r"(\d+)([kKmMgG]+)").unwrap();
    if let Some(captures) = re.captures(size_str) {
        let value: usize = captures[1].parse().map_err(|_| "Failed to parse number")?;
        let unit = &captures[2];
        let multiplier = match unit.to_lowercase().as_str() {
            "k" => 1024,
            "m" => 1024 * 1024,
            "g" => 1024 * 1024 * 1024,
            _ => return Err("Unknown unit"),
        };
        Ok(value * multiplier)
    } else {
       
        Err("Invalid format")
    }
}