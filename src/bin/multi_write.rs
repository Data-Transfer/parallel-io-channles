/// usage:
/// cargo run --features print_ptr --bin multi_read_exact -- x | sort | uniq -c
/// to address of allocated buffer
/// the number of allocated buffers is always equal to the number of readers,
/// regardless of the number of chunks used to read the file and the number
/// of producers
use std::fs::File;
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
    chunk_id: u64,
    producer_id: u64,
    consumers: Senders,
    producer_tx: Sender<Message>,
}
type ProducerId = u64;
type NumProducers = u64;
enum Message {
    Read(ReadData, Buffer),
    End(ProducerId, NumProducers),
}

// Moving a generic Fn instance requires customization
type Producer<T> = dyn Fn(&mut Vec<u8>, T, u64) -> Result<(), String>;
struct FnMove<T> {
    f: Arc<Producer<T>>,
}
impl<T> FnMove<T> {
    fn call(&self, buf: &mut Vec<u8>, t: T, a: u64) -> Result<(), String> {
        (self.f)(buf, t, a)
    }
}
unsafe impl<T> Send for FnMove<T> {}
unsafe impl<T> Sync for FnMove<T> {}

// -----------------------------------------------------------------------------
/// Select target consumer given current producer ID
fn select_tx(_i: usize, prev: usize, c: usize, _p: usize) -> usize {
    (prev + 1) % c
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
    let buffer_size: u64 = std::env::args()
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
    let num_buffers: u64 = if let Some(p) = std::env::args().nth(5) {
        p.parse().expect("Wrong num buffers format")
    } else {
        2 * num_producers
    };
    let producer = |buffer: &mut Vec<u8>, _tag: String, _offset: u64| -> Result<(), String> {
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
        Arc::new(producer),
        data,
        num_buffers,
        buffer_size,
    );
    let len = std::fs::metadata(&filename)
        .expect("Cannot access file")
        .len();
    assert_eq!(bytes_consumed, len as usize);
    std::fs::remove_file(&filename).expect("Cannot delete file");
}

// -----------------------------------------------------------------------------
fn write_to_file<T: 'static + Clone + Send + Sync>(
    filename: &str,
    num_producers: u64,
    num_consumers: u64,
    chunks_per_producer: u64,
    producer: Arc<Producer<T>>,
    client_data: T,
    num_buffers: u64,
    total_size: u64,
) -> usize {
    let producer_chunk_size = (total_size + num_producers - 1) / num_producers;
    let last_producer_chunk_size = total_size - (num_producers - 1) * producer_chunk_size; //num_producers * producer_chunk_size - len + producer_chunk_size;
    let task_chunk_size = (producer_chunk_size + chunks_per_producer - 1) / chunks_per_producer;
    let last_task_chunk_size = producer_chunk_size - (chunks_per_producer - 1) * task_chunk_size; //chunks_per_producer * task_chunk_size -  producer_chunk_size + task_chunk_size;
    let last_prod_task_chunk_size =
        (last_producer_chunk_size + chunks_per_producer - 1) / chunks_per_producer;
    let last_last_prod_task_chunk_size =
        last_producer_chunk_size - (chunks_per_producer - 1) * last_prod_task_chunk_size; //last_prod_task_chunk_size * chunks_per_producer - last_producer_chunk_size + last_prod_task_chunk_size;
    println!(
        r#"
           Total size: {}, 
           Producer chunk size {},  
           Last producer chunk size {},
           Task chunk size: {},
           Last task chunk size: {},
           Last producer task chunk size: {},
           Last last producer task chunk size: {}
           Chunks per producer: {}, 
           Num buffers: {}"#,
        total_size,
        producer_chunk_size,
        last_producer_chunk_size,
        task_chunk_size,
        last_task_chunk_size,
        last_prod_task_chunk_size,
        last_last_prod_task_chunk_size,
        chunks_per_producer,
        num_buffers,
    );
    let file = File::create(filename).expect("Cannot create file");
    file.set_len(total_size).expect("Cannot set file length");
    drop(file);
    let tx_producers = build_producers(num_producers, total_size, 2, producer, client_data);
    let (tx_consumers, consumers_handles) = build_consumers(num_consumers, filename);
    let reserved_size = last_task_chunk_size
        .max(last_last_prod_task_chunk_size)
        .max(task_chunk_size);
    println!("reserved_size: {}", reserved_size);
    launch(
        tx_producers,
        tx_consumers,
        producer_chunk_size,
        last_producer_chunk_size,
        task_chunk_size,
        chunks_per_producer,
        reserved_size as usize,
        num_buffers,
    );

    let mut bytes_consumed = 0;
    for h in consumers_handles {
        let bytes = h.join().expect("Error joining threads");
        bytes_consumed += bytes;
    }
    bytes_consumed
}

// -----------------------------------------------------------------------------
/// Build producers and return array of Sender objects.
fn build_producers<T: 'static + Clone + Sync + Send>(
    num_producers: u64,
    total_size: u64,
    chunks_per_producer: u64,
    f: Arc<Producer<T>>,
    data: T,
) -> Senders {
    let mut tx_producers: Senders = Senders::new();
    // currently producers exit after sending data, and consumers try
    // to send data back to disconnected producers, ignoring the returned
    // send() error
    // another option is to have consumers return and 'End' signal when done
    // consuming data and producers exiting after al the consumers have
    // returned the signal
    for i in 0..num_producers {
        let producer_chunk_size = (total_size + num_producers - 1) / num_producers;
        let _last_producer_chunk_size = total_size - (num_producers - 1) * producer_chunk_size;
        let task_chunk_size = (producer_chunk_size + chunks_per_producer - 1) / chunks_per_producer;
        let (tx, rx) = channel();
        tx_producers.push(tx);
        let mut offset = producer_chunk_size * i;
        let end_offset = (offset + producer_chunk_size).min(total_size);
        //let file = File::open(&filename).expect("Cannot open file");
        use Message::*;
        let cc = FnMove { f: f.clone() };
        let data = data.clone();
        thread::spawn(move || {
            //let mut bf = BufReader::new(file);
            let mut id = chunks_per_producer * i;
            let mut prev_consumer = i as usize;
            while let Ok(Read(mut rd, mut buffer)) = rx.recv() {
                let chunk_size = task_chunk_size.min(end_offset - offset);
                assert!(buffer.capacity() >= chunk_size as usize);
                unsafe {
                    buffer.set_len(chunk_size as usize);
                }
                let num_consumers = rd.consumers.len();
                // to support multiple consumers per producer we need to keep track of
                // the destination, by adding the element into a Set and notify all
                // of them when the producer exits
                let c = select_tx(
                    i as usize,
                    prev_consumer,
                    num_consumers,
                    num_producers as usize,
                );
                //println!("[{}] Sending {} bytes to consumer {}", i, buffer.len(), c);
                prev_consumer = c;
                #[cfg(feature = "print_ptr")]
                println!("{:?}", buffer.as_ptr());

                match cc.call(&mut buffer, data.clone(), offset as u64) {
                    //}, &file, offset)//file.read_exact(&mut buffer) {
                    Err(err) => {
                        //panic!("offset: {} cur_offset: {} buffer.len: {}", rd.offset, rd.cur_offset, buffer.len());
                        panic!("{}", err.to_string());
                    }
                    Ok(()) => {
                        rd.chunk_id = id;
                        id += 1;
                        rd.offset = offset;
                        offset += buffer.len() as u64;
                        //println!("Sending message to consumer {}", c);
                        rd.producer_id = i;
                        rd.consumers[c]
                            .send(Read(rd.clone(), buffer))
                            .expect(&format!("Cannot send buffer"));
                        if offset >= end_offset {
                            // signal the end of stream to consumers
                            (0..rd.consumers.len()).for_each(|x| {
                                //println!("{}>> Sending End of message to consumer {}", i, x);
                                let _ = rd.consumers[x].send(End(i, num_producers));
                            });
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
fn build_consumers(num_consumers: u64, file_name: &str) -> (Senders, Vec<JoinHandle<usize>>) {
    let mut consumers_handles = Vec::new();
    let mut tx_consumers = Vec::new();
    for i in 0..num_consumers {
        let (tx, rx) = channel();
        tx_consumers.push(tx);
        use Message::*;
        let file_name = file_name.to_owned();
        let h = thread::spawn(move || {
            let file = File::options()
                .write(true)
                .open(&file_name)
                .expect("Cannot open file");
            let mut producers_end_signal_count = 0;
            let mut bytes = 0;
            loop {
                // consumers tx endpoints live inside the ReadData instance
                // sent along messages, when producers finish sending data
                // all transmission endpoints die resulting in recv()
                // failing and consumers exiting
                if let Ok(msg) = rx.recv() {
                    match msg {
                        Read(rd, buffer) => {
                            bytes += buffer.len();
                            //println!("{}> Received {} bytes from [{}]", i, buffer.len(), rd.producer_id);
                            //file.seek(SeekFrom::Start(rd.offset)).expect("Cannot move file pointer");
                            write_bytes_at(&buffer, &file, rd.offset)
                                .expect("Cannot write to file");
                            //println!(
                            //    "{}> {} {}/{}",
                            //    i, bytes, producers_end_signal_count, producers_per_consumer
                            //);
                            //println!("{} Sending message to producer", i);
                            if let Err(_err) = rd.producer_tx.send(Read(rd.clone(), buffer)) {
                                // senders might have already exited at this point after having added
                                // data to the queue
                                // from Rust docs
                                //A send operation can only fail if the receiving end of a channel is disconnected, implying that the data could never be received
                                // TBD
                                //break;
                            }
                        }
                        End(_prod_id, num_producers) => {
                            producers_end_signal_count += 1;
                            println!(
                                "{}> received End signal from {} {}/{}",
                                i, _prod_id, producers_end_signal_count, num_producers
                            );
                            if producers_end_signal_count >= num_producers {
                                //println!(
                                //    "{}>> {} {}/{}",
                                //    i, bytes, producers_end_signal_count, producers_per_consumer
                                //);
                                break;
                            }
                        }
                    }
                } else {
                    // we do not care if the communication channel was closed
                    // since it only happen when the producer is finished
                    // of an error elsewhere occurred
                    //println!("{}> Exiting", i);
                    //break;
                }
            }
            //println!("Consumer {} exiting, {} bytes received", i, bytes);
            return bytes;
        });
        consumers_handles.push(h);
    }
    (tx_consumers, consumers_handles)
}

// -----------------------------------------------------------------------------
/// Launch computation by sending messages to transmission endpoints of producer
/// channels.
/// In order to keep memory usage constant, buffers are sent to conumers and
/// returned to the producer who sent them.
/// One producer can send messages to multiple consumers.
/// To allow for asynchronous data consumption, a consumers needs to be able
/// to consume the data while the producer is writing data to a different
/// buffer and therefore more than one buffer per queue is required.
fn launch(
    tx_producers: Senders,
    tx_consumers: Senders,
    producer_chunk_size: u64,
    task_chunk_size: u64,
    last_producer_task_chunk_size: u64,
    chunks_per_producer: u64,
    reserved_size: usize,
    num_buffers: u64,
) {
    let num_producers = tx_producers.len() as u64;
    for i in 0..num_producers {
        let tx = tx_producers[i as usize].clone();
        let offset = (i as u64) * producer_chunk_size;
        //number of messages/buffers to be sent to each producer's queue before
        //the computation starts
        let num_buffers = chunks_per_producer.min(num_buffers);
        for j in 0..num_buffers {
            let mut buffer: Vec<u8> = Vec::new();
            let chunk_size = if i != num_producers - 1 {
                task_chunk_size
            } else {
                last_producer_task_chunk_size
            };
            buffer.reserve(reserved_size);
            unsafe {
                buffer.set_len(chunk_size as usize);
            }
            println!(
                "chunk_size: {}, producer_chunk_size: {}",
                chunk_size, producer_chunk_size
            );
            let rd = ReadData {
                offset: offset,
                chunk_id: chunks_per_producer * i + j,
                producer_tx: tx.clone(),
                consumers: tx_consumers.clone(),
                producer_id: 0, // will be overwritten
            };
            tx.send(Message::Read(rd, buffer)).expect("Cannot send");
        }
    }
}
#[cfg(any(windows))]
fn write_bytes_at(buffer: &Vec<u8>, file: &File, offset: u64) -> Result<(), String> {
    use std::os::windows::fs::FileExt;
    file.seek_write(buffer, offset)
        .map_err(|err| err.to_string())?;
}

#[cfg(any(unix))]
fn write_bytes_at(buffer: &Vec<u8>, file: &File, offset: u64) -> Result<(), String> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buffer, offset)
        .map_err(|err| err.to_string())?;
    Ok(())
}
