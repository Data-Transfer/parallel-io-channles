# `par_io` Parallel I/O library

Simple library to read and write files in parallel implementing the producer-consumer
model.

When reading, the file is subdivided into chunks and each chunk is read by a separate
producer thread which extracts a fixed size buffer from a queue and fills the buffer
with data from file then sends it to a consumer thread which then passes the data
to a callback function. The read buffer size is smaller
or equal to the chunk size and using multiple buffers per producer allows
data generation and consumption to overlap.

When writing, the producer threads send the data returned by the client callback
to the consumer threads which write data to file.
Producers extract fixed size buffers from a queue and pass the buffers to a callback
function which fills the buffers. Buffers are then sent to consumer (writer) threads.

Synchronous calls to `pread` and `pwrite` insider reader or writer threads are used
to transfer data.

No async runtime is used since the actual file I/O is synchronous and task
distribution and execution is controlled by the library through direct calls
to Rust's *thread* and *mpsc* APIs.

## Parallel reading example

```rust
use par_io::read::read_file;
pub fn main() {
    let filename = std::env::args().nth(1).expect("Missing file name");
    let len = std::fs::metadata(&filename)
        .expect("Error reading file size")
        .len();
    let num_producers: u64 = std::env::args()
        .nth(2)
        .expect("Missing num producers")
        .parse()
        .unwrap();
    let num_consumers: u64 = std::env::args()
        .nth(3)
        .expect("Missing num consumers")
        .parse()
        .unwrap();
    let chunks_per_producer: u64 = std::env::args()
        .nth(4)
        .expect("Missing num chunks per producer")
        .parse()
        .unwrap();
    let num_tasks_per_producer: u64 = if let Some(p) = std::env::args().nth(5) {
        p.parse().expect("Wrong num tasks format")
    } else {
        2
    };
    // callback function
    let consume = |buffer: &[u8],   // buffer containing data from file
                   data: &String,   // custom data
                   chunk_id: u64,   // chunk id 
                   num_chunks: u64, // number of chunks per producer
                   _offset: u64     // file offset
     -> Result<usize, String> {
        std::thread::sleep(std::time::Duration::from_secs(1));
        println!(
            "Consumer: {}/{} {} {}",
            chunk_id,
            num_chunks,
            data,
            buffer.len()
        );
        Ok(buffer.len())
    };
    let tag = "TAG".to_string();
    match read_file(
        &filename,
        num_producers,
        num_consumers,
        chunks_per_producer,
        std::sync::Arc::new(consume), // <- callback
        tag,                          // <- client data passed to callback
        num_tasks_per_producer,
    ) {
        Ok(v) => {
            let bytes_consumed = v
                .iter()
                .fold(0, |acc, x| if let (_, Ok(b)) = x { acc + b } else { acc });
            assert_eq!(bytes_consumed, len as usize);
        }
        Err(err) => {
            eprintln!("{}", err.to_string());
        }
    }
```

## Parallel writing example

```rust
use par_io::write::write_to_file;
pub fn main() {
    let buffer_size: usize = std::env::args()
        .nth(1)
        .expect("Missing buffer size")
        .parse()
        .expect("Wrong buffer size format");
    let filename = std::env::args().nth(2).expect("Missing file name");
    let num_producers: u64 = std::env::args()
        .nth(3)
        .expect("Missing num producers")
        .parse()
        .unwrap();
    let num_consumers: u64 = std::env::args()
        .nth(4)
        .expect("Missing num consumers")
        .parse()
        .unwrap();
    let chunks_per_producer: u64 = std::env::args()
        .nth(5)
        .expect("Missing num chunks per producer")
        .parse()
        .unwrap();
    let num_buffers_per_producer: u64 = if let Some(p) = std::env::args().nth(5) {
        p.parse().expect("Wrong num tasks format")
    } else {
        2
    };
    // data generation callback
    let producer = |buffer: &mut Vec<u8>, // buffer to write to
                    _tag: &String,        // client data
                    _offset: u64          // file offset
                   | -> Result<(), String> {
        std::thread::sleep(std::time::Duration::from_secs(1));
        *buffer = vec![1_u8; buffer.len()];
        Ok(())
    };
    let data = "TAG".to_string();
    if let Ok(bytes_consumed) = write_to_file(
        &filename,
        num_producers,
        num_consumers,
        chunks_per_producer,
        std::sync::Arc::new(producer), // <- callback
        data,                          // <- client data passed to callback
        num_buffers_per_producer,
        buffer_size,
    ) {
        let len = std::fs::metadata(&filename)
            .expect("Cannot access file")
            .len();
        assert_eq!(bytes_consumed, len as usize);
        std::fs::remove_file(&filename).expect("Cannot delete file");
    } else {
        eprintln!("Error writing to file");
    }
}
```