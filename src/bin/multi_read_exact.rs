/// usage:
/// cargo run --features print_ptr --bin multi_read_exact -- x | sort | uniq -c
/// to address of allocated buffer
/// the number of allocated buffers is always equal to the number of readers,
/// regardless of the number of chunks used to read the file and the number
/// of producers
use std::fs::File;
//use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::ops::Fn;
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
    size: usize,
    chunk_id: u64,
    num_chunks: u64,
    chunk_size: usize,
    producer_tx: Sender<Message>,
    consumers_per_producer: u64,
    consumers: Senders,
}
enum Message {
    Read(ReadData, Buffer),
    End,
}

//type Consumer<T: 'static + Send + Sync + Clone, R: Sized + 'static + Clone + Sync + Send> = dyn Fn(&[u8], T, u64, u64) ->  R;
//type Consumer<T, R> = fn(&[u8], T, u64, u64) -> R;

// Moving a generic Fn instance requires customization
type Consumer<T, R> = dyn Fn(&[u8], T, u64, u64) -> R;
struct FnMove<T, R> {
    f: Arc<Consumer<T, R>>,
}
impl<T, R> FnMove<T, R> {
    fn call(&self, buf: &[u8], t: T, a: u64, b: u64) -> R {
        (self.f)(buf, t, a, b)
    }
}
unsafe impl<T, R> Send for FnMove<T, R> {}
unsafe impl<T, R> Sync for FnMove<T, R> {}

// -----------------------------------------------------------------------------
/// Select target consumer given current producer ID
fn select_tx(_i: usize, prev: usize, c: usize, _p: usize) -> usize {
    (prev + 1) % c
}

//fn consume(buffer: &[u8], tag: String, chunk_id: u64, num_chunks: u64) -> usize {
//        let s = String::from_utf8_lossy(buffer);
//        println!("{}/{} {} {}", chunk_id, num_chunks, tag, &s[..10]);
//        buffer.len()
//}
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
    let consume = |buffer: &[u8], tag: String, chunk_id: u64, num_chunks: u64| {
        let s = String::from_utf8_lossy(buffer);
        println!("{}/{} {} {}", chunk_id, num_chunks, tag, &s[..10]);
        buffer.len()
    };
    let data = "TAG".to_string();
    let bytes_consumed = read_file(
        &filename,
        num_producers,
        num_consumers,
        3,
        Arc::new(consume),
        data,
    );
    assert_eq!(bytes_consumed, len as usize);
}
// -----------------------------------------------------------------------------
fn read_file<T: 'static + Clone + Send + Sync, R: 'static + Clone + Sync + Send>(
    filename: &str,
    num_producers: u64,
    num_consumers: u64,
    chunks_per_task: u64,
    consumer: Arc<Consumer<T, R>>,
    client_data: T,
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
    let (tx_consumers, consumers_handles) = build_consumers(num_consumers, consumer, client_data);
    launch(tx_producers, tx_consumers, chunk_size, task_chunk_size, len);

    let mut bytes_consumed = 0;
    let mut ret = Vec::new();
    for h in consumers_handles {
        let (bytes, chunks) = h.join().expect("Error joining threads");
        bytes_consumed += bytes;
        ret.extend(chunks);
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
        //let file = File::open(&filename).expect("Cannot open file");
        let mut file = File::open(&filename).expect("Cannot open file");
        use Message::*;
        thread::spawn(move || {
            //let mut bf = BufReader::new(file);
            let mut consumers = std::collections::HashSet::new();
            let mut prev_consumer = i as usize;
            let mut cur_offset: i64 = -1;
            while let Ok(Read(mut rd, mut buffer)) = rx.recv() {
                if cur_offset < 0 { cur_offset = rd.offset as i64; }
                let end_offset = (rd.offset + rd.size as u64).min(len);
                let cur_off = cur_offset as u64;
                // if file_length - offset < 2 * chunk_length set chunk_size to
                // length - offset
                if end_offset - cur_off < 2 * rd.chunk_size as u64 {
                    rd.chunk_size = (end_offset - cur_off) as usize;
                }
                assert!(buffer.capacity() >= rd.chunk_size);
                unsafe {
                    buffer.set_len(rd.chunk_size);
                }
                //bf.seek(SeekFrom::Start(rd.cur_offset)).unwrap();
                file.seek(SeekFrom::Start(cur_off)).unwrap();
                let num_consumers = rd.consumers.len();
                // to support multiple consumers per producer we need to keep track of
                // the destination, by adding the element into a Set and notify all
                // of them when the producer exits
                 
                let c = select_tx(i as usize, prev_consumer, num_consumers, num_producers as usize);
                consumers.insert(c);
                prev_consumer = c;
                #[cfg(feature = "print_ptr")]
                println!("{:?}", buffer.as_ptr());

                //match bf.read_exact(&mut buffer) {
                match file.read_exact(&mut buffer) {
                    Err(err) => {
                        //panic!("offset: {} cur_offset: {} buffer.len: {}", rd.offset, rd.cur_offset, buffer.len());
                        panic!("{}", err.to_string());
                    }
                    Ok(_s) => {
                        rd.chunk_id += 1;
                        cur_offset += buffer.len() as i64;
                        //println!("Sending message to consumer {}", c);
                        rd.consumers[c]
                            .send(Read(rd.clone(), buffer))
                            .expect(&format!("Cannot send buffer"));
                        if cur_off >= end_offset {
                            consumers.iter().for_each(|i|
                                                rd.consumers[*i]
                                                    .send(End)
                                                    .expect(&format!("Cannot send buffer")));
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
fn build_consumers<T: 'static + Clone + Sync + Send, R: 'static + Clone + Sync + Send>(
    num_consumers: u64,
    f: Arc<Consumer<T, R>>,
    data: T,
) -> (Senders, Vec<JoinHandle<(usize, Vec<R>)>>) {
    let mut consumers_handles = Vec::new();
    let mut tx_consumers = Vec::new();
    for i in 0..num_consumers {
        let (tx, rx) = channel();
        tx_consumers.push(tx);
        use Message::*;
        let cc = FnMove { f: f.clone() };
        let data = data.clone();
        let h = thread::spawn(move || {
            let mut ret = Vec::new();
            let mut consumers = 0;
            let mut consumers_per_producer = 0;
            let mut bytes = 0;
            loop {
                if let Ok(msg) = rx.recv() {
                    match msg {
                        Read(rd, buffer) => {
                            bytes += buffer.len();
                            consumers_per_producer = rd.consumers_per_producer;
                            ret.push(cc.call(&buffer, data.clone(), rd.chunk_id, rd.num_chunks));
                            //ret.push(f(&buffer, data.clone(), rd.chunk_id, rd.num_chunks));
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
            return (bytes, ret);
        });
        consumers_handles.push(h);
    }
    (tx_consumers, consumers_handles)
}

// -----------------------------------------------------------------------------
fn launch(
    tx_producers: Senders,
    tx_consumers: Senders,
    chunk_size: u64,
    task_chunk_size: u64,
    total_size: u64,
) {
    let num_producers = tx_producers.len() as u64;
    let num_consumers = tx_consumers.len() as u64;
    let chunks_per_task = if task_chunk_size % chunk_size == 0 {
        task_chunk_size / chunk_size
    } else {
        (task_chunk_size + chunk_size - 1) / chunk_size - 1
    };
    let last_task_chunk_size = task_chunk_size - (task_chunk_size * num_producers - total_size);
    let last_chunks_per_task = if last_task_chunk_size % chunk_size == 0 {
        last_task_chunk_size / chunk_size
    } else {
        (last_task_chunk_size + chunk_size - 1) / chunk_size - 1
    };
    let total_chunks = chunks_per_task * (num_producers - 1) + last_chunks_per_task;

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
            size: task_chunk_size as usize,
            chunk_id: chunks_per_task * i,
            num_chunks: total_chunks,
            chunk_size: chunk_size as usize,
            producer_tx: tx.clone(),
            consumers: tx_consumers.clone(),
            consumers_per_producer: num_producers / num_consumers as u64,
        };
        tx.send(Message::Read(rd, buffer)).expect("Cannot send");
    }
}
