
//------------------------------------------------------------------------------
use par_io::par_read::read_file;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;

#[derive(Clone)]
struct Data {
    tx: Sender<String>,
    msg: String
}


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
    let consume = |buffer: &[u8], data: &Data, chunk_id: u64, num_chunks: u64| {
        std::thread::sleep(std::time::Duration::from_secs(1));
        println!(
            "Consumer: {}/{} {} {}",
            chunk_id,
            num_chunks,
            data.msg,
            buffer.len()
        );
        data.tx.send(buffer.len().to_string()).expect("Error sending");
        buffer.len()
    };
    let (tx,rx) = channel::<String>();
    let tag = "TAG".to_string();
    let bytes_consumed = read_file(
        &filename,
        num_producers,
        num_consumers,
        chunks_per_producer,
        std::sync::Arc::new(consume),
        Data{tx: tx, msg: tag},
        num_tasks_per_producer,
    );
    while let Ok(msg) = rx.recv() {
        println!("{}", msg);
    }
    assert_eq!(bytes_consumed, len as usize);
}