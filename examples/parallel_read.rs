// -----------------------------------------------------------------------------
//! Separate file reading from data consumption using a fixed amount of memory.
//! * thread *i* reads data from file and sends it to thread *j*
//! * thread *j* consumes data passing it to the client callback function and
//!    and sends consumed buffer back to thread *i* so that it can be reused
//! The total number of buffers used equals the number of producers times the
//! number of buffers per producer, regardless of the number of chunks read.
//------------------------------------------------------------------------------
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
    let consume = |buffer: &[u8],
                   data: &String,
                   chunk_id: u64,
                   num_chunks: u64,
                   _offset: u64|
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
        std::sync::Arc::new(consume),
        tag,
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
}
