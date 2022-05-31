//! A simple example of parallel writing to file. 
//! Data is generated in producer callback function and sent to writer threads
//! 
//! Input:
//! 
//! * memory buffer size
//! * output file name
//! * number of producer threads
//! * number of consumer threads, 
//! * number of chunks (== tasks) per producer
//! * number of buffers per producer
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
    let producer = |buffer: &mut Vec<u8>, _tag: &String, _offset: u64| -> Result<(), String> {
        std::thread::sleep(std::time::Duration::from_secs(1));
        *buffer = vec![1_u8; buffer.len()];
        Ok(())
    };
    let data = "TAG".to_string();
    match write_to_file(
        &filename,
        num_producers,
        num_consumers,
        chunks_per_producer,
        std::sync::Arc::new(producer),
        data,
        num_buffers_per_producer,
        buffer_size,
    ) {
        Ok(bytes_consumed) => {
            let len = std::fs::metadata(&filename)
                .expect("Cannot access file")
                .len();
            assert_eq!(bytes_consumed, len as usize);
            std::fs::remove_file(&filename).expect("Cannot delete file");
        },
        Err(err) => {
            eprintln!("{:?}", err);
        }
    }
}
