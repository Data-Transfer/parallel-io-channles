/// @todo: Handle non divisible buffer length
/// use:
/// cargo run --features print_ptr --bin multi_read_exact -- x | sort | uniq -c
/// to address of allocated buffer
/// the number of allocated buffers is always equal to the number of readers,
/// regardless of the number of chunks used to read the file and the number
/// of producers
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

// -----------------------------------------------------------------------------
type Senders = Vec<Sender<Message>>;
type Buffer = Vec<u8>;
type Offset = u64;
#[derive(Clone)]
struct ReadData {
    offset: Offset,
    cur_offset: Offset,
    size: usize,
    chunk_size: usize,
    producer_tx: Sender<Message>,
    consumers_per_producer: u64,
    consumers: Senders,
}
enum Message {
    Read(ReadData, Buffer),
    End,
}

// -----------------------------------------------------------------------------
/// Select target consumer given current producer ID
fn select_tx(i: usize, c: usize, _p: usize) -> usize {
    i % c
}

// -----------------------------------------------------------------------------
/// Separate file read from data consumption using a fixed amount of memory.
/// * thread 1 reads data from file and sends it to thread 2
/// * thread 2 consumes data and sends consumed buffer back to thread 1 so that
///   it can be reused
/// The sender sends the buffer and a copy of the sender instance to be used
/// to return the buffer to he sender. This way only the number of buffers equals
/// the number of producers regardless of the number chunks read.
/// Current requirement is that:
/// * *number of producers* >= *number of consumers*
/// Total memory used = # producers x chunk_size
fn main() {
    let filename = std::env::args().nth(1).expect("Missing file name");
    let len = std::fs::metadata(&filename)
        .expect("Error reading file size")
        .len();
    let num_producers = 4;
    let num_consumers = 2;
    let consume = |buffer: &[u8]| {
        let s = String::from_utf8_lossy(buffer);
        println!("{}", &s[..10]);
    };
    let bytes_consumed = read_file(&filename, num_producers, num_consumers, 3, Arc::new(consume));
    assert_eq!(bytes_consumed, len as usize);
}

// -----------------------------------------------------------------------------
fn read_file(
    filename: &str,
    num_producers: u64,
    num_consumers: u64,
    chunks_per_task: u64,
    f: Arc<dyn Fn(&[u8]) -> () + Send + Sync>,
) -> usize {
    let len = std::fs::metadata(&filename)
        .expect("Error reading file size")
        .len();
    let task_chunk_size = (len + num_producers - 1) / num_producers;
    let chunk_size = (task_chunk_size + chunks_per_task - 1) / chunks_per_task;

    println!(
        "File size: {}, Thread chunk size {},  Task chunk size: {}",
        len, task_chunk_size, chunk_size
    );

    let tx_producers = build_producers(num_producers, &filename);
    let (tx_consumers, consumers_handles) = build_consumers(num_consumers, f);
    launch(tx_producers, tx_consumers, chunk_size, task_chunk_size);

    let mut bytes_consumed = 0;

    for h in consumers_handles {
        bytes_consumed += h.join().expect("Error joining threads");
    }
    bytes_consumed
}

// -----------------------------------------------------------------------------
/// Callback receiving data read from file and sent from producer to consumer
//fn consume(buffer: &[u8]) {
//    let s = String::from_utf8_lossy(buffer);
//    println!("{}", &s[..10]);
//}

// -----------------------------------------------------------------------------
/// Build producers and return array of Sender objects.
fn build_producers(num_producers: u64, filename: &str) -> Senders {
    let len = std::fs::metadata(filename)
        .expect("Cannot read metadata")
        .len();
    let mut tx_producers: Senders = Senders::new();
    // currently producers exit after sending data, and consumers try
    // to send data back to disconnected producers, ignoring the returned
    // send() error
    // another option is to have consumers return and 'End' signal when done
    // consuming data and producers exiting after al the consumers have
    // returned the signal
    for i in 0..num_producers {
        let (tx, rx) = channel();
        tx_producers.push(tx);
        let file = File::open(&filename).expect("Cannot open file");
        use Message::*;
        thread::spawn(move || {
            let mut bf = BufReader::new(file);
            while let Ok(Read(mut rd, mut buffer)) = rx.recv() {
                if rd.cur_offset - rd.offset >= rd.size as u64 {
                    break;
                }
                let end_offset = (rd.offset + rd.size as u64).min(len);
                // if file_length - offset < 2 * chunk_length set chunk_size to
                // length - offset
                if end_offset - rd.cur_offset < 2 * rd.chunk_size as u64 {
                    rd.chunk_size = (end_offset - rd.cur_offset) as usize;
                }
                assert!(buffer.capacity() >= rd.chunk_size);
                unsafe {
                    buffer.set_len(rd.chunk_size);
                }
                bf.seek(SeekFrom::Start(rd.cur_offset)).unwrap();
                let num_consumers = rd.consumers.len();
                // to support multiple consumers per producer we need to keep track of
                // the destination, by adding the element into a Set and notify all
                // of them when the producer exits
                let c = select_tx(i as usize, num_consumers, num_producers as usize);
                #[cfg(feature = "print_ptr")]
                println!("{:?}", buffer.as_ptr());

                match bf.read_exact(&mut buffer) {
                    Err(err) => {
                        //panic!("offset: {} cur_offset: {} buffer.len: {}", rd.offset, rd.cur_offset, buffer.len());
                        panic!("{}", err.to_string());
                    }
                    Ok(_s) => {
                        rd.cur_offset += buffer.len() as u64;
                        //println!("Sending message to consumer {}", c);
                        rd.consumers[c]
                            .send(Read(rd.clone(), buffer))
                            .expect(&format!("Cannot send buffer"));
                        if rd.cur_offset >= end_offset {
                            rd.consumers[c]
                                .send(End)
                                .expect(&format!("Cannot send buffer"));
                            break;
                        }
                    }
                }
            }
            println!("Producer {} exiting", i);
            return;
        });
    }
    tx_producers
}

// -----------------------------------------------------------------------------
/// Build consumers and return tuple of (Sender objects, JoinHandles)
fn build_consumers(
    num_consumers: u64,
    f: Arc<dyn Fn(&[u8]) -> () + Sync + Send>,
) -> (Senders, Vec<JoinHandle<usize>>) {
    let mut consumers_handles = Vec::new();
    let mut tx_consumers = Vec::new();
    for i in 0..num_consumers {
        let (tx, rx) = channel();
        tx_consumers.push(tx);
        use Message::*;
        let f = f.clone();
        let h = thread::spawn(move || loop {
            let mut consumers = 0;
            let mut consumers_per_producer = 0;
            let mut bytes = 0;
            loop {
                if let Ok(msg) = rx.recv() {
                    match msg {
                        Read(rd, buffer) => {
                            bytes += buffer.len();
                            consumers_per_producer = rd.consumers_per_producer;
                            f(&buffer);
                            println!("{}> {} {}/{}", i, bytes, consumers, consumers_per_producer);
                            //println!("{} Sending message to producer", i);
                            if let Err(_err) = rd.producer_tx.send(Read(rd.clone(), buffer)) {
                                // senders might have already exited at this point after having added
                                // data to the queue
                                // from Rust docs
                                //A send operation can only fail if the receiving end of a channel is disconnected, implying that the data could never be received
                                // TBD
                            }
                        }
                        End => {
                            consumers += 1;
                            if consumers >= consumers_per_producer {
                                println!(
                                    "{}> {} {}/{}",
                                    i, bytes, consumers, consumers_per_producer
                                );
                                break;
                            }
                        }
                    }
                } else {
                    break;
                }
            }
            println!("Consumer {} exiting", i);
            return bytes;
        });
        consumers_handles.push(h);
    }
    (tx_consumers, consumers_handles)
}

// -----------------------------------------------------------------------------
fn launch(tx_producers: Senders, tx_consumers: Senders, chunk_size: u64, task_chunk_size: u64) {
    let num_producers = tx_producers.len() as u64;
    let num_consumers = tx_consumers.len() as u64;
    for i in 0..num_producers {
        let tx = tx_producers[i as usize].clone();
        let mut buffer: Vec<u8> = Vec::new();
        //actual number is lower, but quicker to do this
        buffer.reserve(2 * chunk_size as usize);
        unsafe {
            buffer.set_len(chunk_size as usize);
        }
        let offset = i * task_chunk_size as u64;
        let rd = ReadData {
            offset: offset,
            cur_offset: offset,
            size: task_chunk_size as usize,
            chunk_size: chunk_size as usize,
            producer_tx: tx.clone(),
            consumers: tx_consumers.clone(),
            consumers_per_producer: num_producers / num_consumers as u64,
        };
        tx.send(Message::Read(rd, buffer)).expect("Cannot send");
    }
}
