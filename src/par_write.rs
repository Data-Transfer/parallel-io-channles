/// Parallel async file write from memory buffers.
/// The number of allocated buffers is always equal to the number of buffers
/// per producer times the number of producers; all buffers are allocated
/// once before the are executed.
use std::fs::File;
use std::ops::Fn;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

// -----------------------------------------------------------------------------
// TYPES

type Senders = Vec<Sender<Message>>;
type Buffer = Vec<u8>;
type Offset = u64;
#[derive(Clone)]
struct Config {
    offset: Offset,
    consumers: Senders,
    producer_tx: Sender<Message>,
}
// Using the same type to communicate between producers and consumers.
type ProducerConfig = Config;
type ConsumerConfig = Config;
type ProducerId = u64;
type NumProducers = u64;
enum Message {
    Consume(ConsumerConfig, Buffer), // sent to consumers
    Produce(ProducerConfig, Buffer), // sent to producers
    End(ProducerId, NumProducers),   // sent from producers to all consumers
                                     // to signal end of transmission
}

// Moving a generic Fn instance requires customization
type Producer<T> = dyn Fn(&mut Vec<u8>, &T, u64) -> Result<(), String>;
struct FnMove<T> {
    f: Arc<Producer<T>>,
}

/// Fn is wrapped inside an FnMove struct so that it can be moved
impl<T> FnMove<T> {
    fn call(&self, buf: &mut Vec<u8>, t: &T, a: u64) -> Result<(), String> {
        (self.f)(buf, t, a)
    }
} 

unsafe impl<T> Send for FnMove<T> {}

// -----------------------------------------------------------------------------
/// Select target consumer given current producer ID
fn select_tx(
    _i: usize,
    previous_consumer_id: usize,
    num_consumers: usize,
    _num_producers: usize,
) -> usize {
    (previous_consumer_id + 1) % num_consumers
}


// -----------------------------------------------------------------------------
/// Write data to file.
/// ```no_run
/// |||..........|.....||..........|.....||...>> ||...|.|||
///    <---1----><--2->                            <3><4>
///    <-------5------>                            <--6->
///    <-------------------------7---------------------->
/// ```
/// 1. task_chunk_size
/// 2. last_task_chunk_size
/// 3. last_producer_task_chunk_size
/// 4. last_producer_last_task_chunk_size
/// 5. producer_chunk_size
/// 6. last_producer_chunk_size
/// 7. total_size
pub fn write_to_file<T: 'static + Clone + Send>(
    filename: &str,
    num_producers: u64,
    num_consumers: u64,
    chunks_per_producer: u64,
    producer: Arc<Producer<T>>,
    client_data: T,
    num_tasks_per_producer: u64,
    total_size: usize,
) -> usize {
    let total_size = total_size as u64;
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
           Tasks per producer: {}"#,
        total_size,
        producer_chunk_size,
        last_producer_chunk_size,
        task_chunk_size,
        last_task_chunk_size,
        last_prod_task_chunk_size,
        last_last_prod_task_chunk_size,
        chunks_per_producer,
        num_tasks_per_producer,
    );
    let file = File::create(filename).expect("Cannot create file");
    file.set_len(total_size).expect("Cannot set file length");
    drop(file);
    let tx_producers = build_producers(
        num_producers,
        total_size,
        chunks_per_producer,
        producer,
        client_data,
    );
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
        num_tasks_per_producer,
    );

    let mut bytes_consumed = 0;
    for h in consumers_handles {
        match h.join().expect("Error joining threads") {
            Ok(bytes) => {bytes_consumed += bytes;},
            Err(err) => {eprintln!("Error: {}", err.to_string())}
        }
    }
    bytes_consumed
}

// -----------------------------------------------------------------------------
/// Build producers and return array of Sender objects.
fn build_producers<T: 'static + Clone + Send>(
    num_producers: u64,
    total_size: u64,
    chunks_per_producer: u64,
    f: Arc<Producer<T>>,
    data: T,
) -> Senders {
    let mut tx_producers: Senders = Senders::new();
    let producer_chunk_size = (total_size + num_producers - 1) / num_producers;
    let last_producer_chunk_size = total_size - (num_producers - 1) * producer_chunk_size;
    let task_chunk_size = (producer_chunk_size + chunks_per_producer - 1) / chunks_per_producer;
    let last_prod_task_chunk_size =
        (last_producer_chunk_size + chunks_per_producer - 1) / chunks_per_producer;
    // currently producers exit after sending all data, and consumers might try
    // to send data back to disconnected producers, ignoring the returned
    // send() error;
    // another option is to have consumers return and 'End' signal when done
    // consuming data and producers exiting after al the consumers have
    // returned the signal
    for i in 0..num_producers {
        let (tx, rx) = channel();
        tx_producers.push(tx);
        let mut offset = producer_chunk_size * i;
        let end_offset = if i != num_producers - 1 {
            offset + producer_chunk_size
        } else {
            offset + last_producer_chunk_size
        };
        use Message::*;
        let cc = FnMove { f: f.clone() };
        let data = data.clone();
        thread::spawn(move || {
            //let mut bf = BufReader::new(file);
            let mut prev_consumer = i as usize;
            while let Ok(Produce(mut cfg, mut buffer)) = rx.recv() {
                let chunk_size = if i != num_producers - 1 {
                    task_chunk_size.min(end_offset - offset)
                } else {
                    last_prod_task_chunk_size.min(end_offset - offset)
                };
                //println!("producer_chunk_size: {} chunks_per_producers {} task_chunk_size: {} capacity: {}, chunk_size: {}", producer_chunk_size, chunks_per_producer, task_chunk_size, buffer.capacity(), chunk_size);
                assert!(buffer.capacity() >= chunk_size as usize);
                unsafe {
                    buffer.set_len(chunk_size as usize);
                }
                let num_consumers = cfg.consumers.len();
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

                match cc.call(&mut buffer, &data, offset as u64) {
                    //}, &file, offset)//file.read_exact(&mut buffer) {
                    Err(err) => {
                      return Err(err.to_string())  //panic!("offset: {} cur_offset: {} buffer.len: {}", cfg.offset, cfg.cur_offset, buffer.len());
                    },
                    Ok(()) => {
                        cfg.offset = offset;
                        offset += buffer.len() as u64;
                        //println!("Sending message to consumer {}", c);
                        //cfg.producer_id = i;
                        if let Err(err) = cfg.consumers[c] 
                            .send(Consume(cfg.clone(), buffer)) {
                            return Err(format!("Cannot send buffer to consumer - {}", err.to_string()));
                        }
                        if offset >= end_offset {
                            // signal the end of stream to consumers
                            (0..cfg.consumers.len()).for_each(|x| {
                                //println!("{}>> Sending End of message to consumer {}", i, x);
                                // consumer might heve exited already
                                let _ = cfg.consumers[x].send(End(i, num_producers));
                            });
                            break;
                        }
                    }
                }
            }
            println!("Producer {} exiting", i);
            return Ok(())
        });
    }
    tx_producers
}

// -----------------------------------------------------------------------------
/// Build consumers and return tuple of (Sender objects, JoinHandles)
fn build_consumers(num_consumers: u64, file_name: &str) -> (Senders, Vec<JoinHandle<Result<usize, String>>>) {
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
                        Consume(cfg, buffer) => {
                            bytes += buffer.len();
                            //println!("{}> Received {} bytes from [{}]", i, buffer.len(), cfg.producer_id);
                            //file.seek(SeekFrom::Start(cfg.offset)).expect("Cannot move file pointer");
                            if let Err(err) = write_bytes_at(&buffer, &file, cfg.offset) {
                                return Err(format!("Cannot write to file - {}", err.to_string()));
                            }
                            //println!(
                            //    "{}> {} {}/{}",
                            //    i, bytes, producers_end_signal_count, producers_per_consumer
                            //);
                            //println!("{} Sending message to producer", i);
                            if let Err(_err) = cfg.producer_tx.send(Produce(cfg.clone(), buffer)) {
                                // senders might have already exited at this point after having added
                                // data to the queue
                                // from Rust docs
                                //A send operation can only fail if the receiving end of a channel is disconnected, implying that the data could never be received
                                // TBD
                                //break;
                            }
                        },
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
                        _ => {
                            panic!("Wrong message type");
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
            return Ok(bytes);
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
/// to consume the data in a bffer while the producer is writing data to a different
/// buffer and therefore more than one buffer per producer is required for
/// the operation to perform asynchronously.
fn launch(
    tx_producers: Senders,
    tx_consumers: Senders,
    producer_chunk_size: u64,
    task_chunk_size: u64,
    last_producer_task_chunk_size: u64,
    chunks_per_producer: u64,
    reserved_size: usize,
    num_tasks_per_producer: u64,
) {
    //number of read tasks performer/producer = number of buffers sent to
    //producer
    let num_buffers_per_producer = num_tasks_per_producer;
    let num_producers = tx_producers.len() as u64;
    for i in 0..num_producers {
        let tx = tx_producers[i as usize].clone();
        let offset = (i as u64) * producer_chunk_size;
        //number of messages/buffers to be sent to each producer's queue before
        //the computation starts
        let num_buffers = chunks_per_producer.min(num_buffers_per_producer);
        for _ in 0..num_buffers {
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
            let cfg = ProducerConfig {
                offset: offset,
                producer_tx: tx.clone(),
                consumers: tx_consumers.clone(),
            };
            tx.send(Message::Produce(cfg, buffer)).expect("Cannot send");
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