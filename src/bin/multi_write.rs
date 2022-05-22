// -----------------------------------------------------------------------------
/// Separate file write from data prduction using a fixed amount of memory.
/// * thread 1 reads generates data and sends it to thread 2
/// * thread 2 writes data to file sends consumed buffer back to thread 1 so that
///   it can be reused
/// The sender sends the buffer and a copy of the sender instance to be used
/// to return the buffer to he sender. This way only the number of buffers equals
/// the number of producers times the number of buffers per producer,
/// regardless of the number of chunks generated.
use par_io::par_write::write_to_file;
fn main() {
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
    let num_tasks_per_producer: u64 = if let Some(p) = std::env::args().nth(5) {
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
    let bytes_consumed = write_to_file(
        &filename,
        num_producers,
        num_consumers,
        chunks_per_producer,
        std::sync::Arc::new(producer),
        data,
        num_tasks_per_producer,
        buffer_size,
    );
    let len = std::fs::metadata(&filename)
        .expect("Cannot access file")
        .len();
    assert_eq!(bytes_consumed, len as usize);
    std::fs::remove_file(&filename).expect("Cannot delete file");
}