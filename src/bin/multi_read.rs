
//------------------------------------------------------------------------------
use par_io::par_read::read_file;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;

#[derive(Clone)]
struct Data {
    _tx: Sender<String>,
    msg: String
}

// -----------------------------------------------------------------------------
/// Separate file read from data consumption using a fixed amount of memory.
/// * thread 1 reads data from file and sends it to thread 2
/// * thread 2 consumes data and sends consumed buffer back to thread 1 so that
///   it can be reused
/// The sender sends the buffer and a copy of the sender instance to be used
/// to return the buffer to he sender. This way only the number of buffers equals
/// the number of producers times the number of buffers per producer,
/// regardless of the number of chunks generated.
fn main() {
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
    let consume = |buffer: &[u8], data: &Data, chunk_id: u64, num_chunks: u64| -> Result<usize, String>{
        std::thread::sleep(std::time::Duration::from_secs(1));
        println!(
            "Consumer: {}/{} {} {}",
            chunk_id,
            num_chunks,
            data.msg,
            buffer.len()
        );
        //_data.tx.send(buffer.len().to_string()).expect("Error sending");
        Ok(buffer.len())
    };
    let (tx,_rx) = channel::<String>();
    //std::thread::spawn( move || {
    //    //only move rx
    //    let rx = rx;
    //    while let Ok(msg) = rx.recv() { 
    //        println!("{}", msg);
    //    }});
    let tag = "TAG".to_string();
    match read_file(
        &filename,
        num_producers,
        num_consumers,
        chunks_per_producer,
        std::sync::Arc::new(consume),
        Data{_tx: tx, msg: tag},
        num_tasks_per_producer,
    ) { 
        Ok(v) => {
            let bytes_consumed = v.iter().fold(0, |acc, x| if let (_,Ok(b)) = x { acc + b } else {acc} ); 
            assert_eq!(bytes_consumed, len as usize);
        },
        Err(err) => { eprintln!("{}", err.to_string()); }
    }
}