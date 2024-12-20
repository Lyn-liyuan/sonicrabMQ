# SonicrabMQ: A Zero-Copy Message Queue in Rust
A lightweight, high-performance message queue implementation using Rust.
Inspired by Kafka, this implementation leverages mmap (memory-mapped files) for efficient index management and Linux sendfile for zero-copy data transfer.

## Features

### 🚀 Zero-Copy Data Transfer: Uses Linux sendfile for efficient data transmission without extra memory copy between kernel and user space.

### 📄 File-Based Storage:
* Data Files (*.data) store messages with headers indicating length and offsets.
Index Files (*.index) store fixed-size entries for quick data positioning.
### 🧠 Memory-Mapped Index:
* Index files are managed using mmap, allowing high-speed access and updates.
### Dynamic expansion of index files ensures flexibility with growing data.
* 🔄 Automatic File Rotation: New files are created when a data file exceeds the size threshold (default: 1 GB).
* 🔍 Efficient Data Lookup: Quickly locate and read messages using stored offsets.
* 🔧 Clean & Modular Design: Easy to extend and integrate into other systems.

## Evaluation

We provide two python scripts for compression testing.

* producer.py will launch 10 processes, which will push 100K messages with payloads ranging from 5KB to 1MB of random data to 5 brokers. It will assign 2 processes to each broker.
* consomer.py will launch 5 processes, those will pull messages from 5 broker, one process for one broker.

## License
This project is licensed under the MIT License. 