/// Parallel async file read into memory buffers.
/// The number of allocated buffers is always equal to the number of buffers
/// per producer times the number of producers; all buffers are allocated
/// once before threads are executed.
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
#[derive(Clone)]
struct Config {
    chunk_id: u64,
    num_chunks: u64,
    producer_tx: Sender<Message>,
    consumers: Senders,
    offset: u64,
}
type ProducerConfig = Config;
type ConsumerConfig = Config;
type ProducerId = u64;
type NumProducers = u64;
enum Message {
    //Error(String), // sent when producer encounters and error
    Consume(ConsumerConfig, Buffer), // sent to consumers
    Produce(ProducerConfig, Buffer), // sent to producers
    End(ProducerId, NumProducers),   // sent from producers to all consumers
                                     // to signal end of transmission
}

// Moving a generic Fn instance requires customization
type Consumer<T, R> = dyn Fn(&[u8], &T, u64, u64, u64) -> R;
struct FnMove<T, R> {
    f: Arc<Consumer<T, R>>,
}
impl<T, R> FnMove<T, R> {
    fn call(&self, buf: &[u8], t: &T, a: u64, b: u64, c: u64) -> R {
        (self.f)(buf, t, a, b, c)
    }
}
unsafe impl<T, R> Send for FnMove<T, R> {}

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
/// Separate file write from data prduction using a fixed amount of memory.
/// * thread 1 reads generates data and sends it to thread 2
/// * thread 2 writes data to file sends consumed buffer back to thread 1 so that
///   it can be reused
/// The sender sends the buffer and a copy of the sender instance to be used
/// to return the buffer to he sender. This way only the number of buffers equals
/// the number of producers times the number of buffers per producer,
/// regardless of the number of chunks generated.

// -----------------------------------------------------------------------------
/// Write data to file.
/// ```ignore
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
pub fn read_file<T: 'static + Clone + Send, R: 'static + Clone + Sync + Send>(
    filename: &str,
    num_producers: u64,
    num_consumers: u64,
    chunks_per_producer: u64,
    consumer: Arc<Consumer<T, R>>,
    client_data: T,
    num_tasks_per_producer: u64,
) -> Result<Vec<(u64, R)>, String> {
    let total_size = match std::fs::metadata(&filename) {
        Ok(m) => m.len(),
        Err(err) => {
            return Err(err.to_string());
        }
    };
    let producer_chunk_size = (total_size + num_producers - 1) / num_producers;
    let last_producer_chunk_size = total_size - (num_producers - 1) * producer_chunk_size; //num_producers * producer_chunk_size - total_size + producer_chunk_size;
    let task_chunk_size = (producer_chunk_size + chunks_per_producer - 1) / chunks_per_producer;
    let last_task_chunk_size = producer_chunk_size - (chunks_per_producer - 1) * task_chunk_size; //chunks_per_producer * task_chunk_size -  producer_chunk_size + task_chunk_size;
    let last_prod_task_chunk_size =
        (last_producer_chunk_size + chunks_per_producer - 1) / chunks_per_producer;
    let last_last_prod_task_chunk_size =
        last_producer_chunk_size - (chunks_per_producer - 1) * last_prod_task_chunk_size; //last_prod_task_chunk_size * chunks_per_producer - last_producer_chunk_size + last_prod_task_chunk_size;
                                                                                          /*println!(
                                                                                              r#"
                                                                                                 File size: {},
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
                                                                                              num_tasks_per_producer
                                                                                          );*/

    let tx_producers = build_producers(num_producers, chunks_per_producer, &filename)?;
    let (tx_consumers, consumers_handles) = build_consumers(num_consumers, consumer, client_data);
    let reserved_size = last_task_chunk_size
        .max(last_last_prod_task_chunk_size)
        .max(task_chunk_size);
    //println!("reserved_size: {}", reserved_size);
    if let Err(err) = launch(
        tx_producers,
        tx_consumers,
        task_chunk_size,
        last_prod_task_chunk_size,
        chunks_per_producer,
        reserved_size as usize,
        num_tasks_per_producer,
    ) {
        return Err(err);
    }

    let mut ret = Vec::new();
    for h in consumers_handles {
        match h.join() {
            Ok(chunks) => {
                ret.extend(chunks);
            }
            Err(err) => {
                return Err(format!("{:?}", err));
            }
        }
    }
    Ok(ret)
}

// -----------------------------------------------------------------------------
/// Build producers and return array of Sender objects.
fn build_producers(
    num_producers: u64,
    chunks_per_producer: u64,
    filename: &str,
) -> Result<Senders, String> {
    let total_size = std::fs::metadata(filename)
        .map_err(|err| err.to_string())?
        .len();
    let mut tx_producers: Senders = Senders::new();
    let producer_chunk_size = (total_size + num_producers - 1) / num_producers;
    let last_producer_chunk_size = total_size - (num_producers - 1) * producer_chunk_size;
    let task_chunk_size = (producer_chunk_size + chunks_per_producer - 1) / chunks_per_producer;
    let last_prod_task_chunk_size =
        (last_producer_chunk_size + chunks_per_producer - 1) / chunks_per_producer;
    // currently producers exit after sending data, and consumers try
    // to send data back to disconnected producers, ignoring the returned
    // send() error
    // another option is to have consumers return and 'End' signal when done
    // consuming data and producers exiting after al the consumers have
    // returned the signal
    for i in 0..num_producers {
        let (tx, rx) = channel();
        tx_producers.push(tx);
        let mut chunk_id = chunks_per_producer * i;
        let mut offset = producer_chunk_size * i;
        let end_offset = if i != num_producers - 1 {
            offset + producer_chunk_size
        } else {
            offset + last_producer_chunk_size
        };
        let file = File::open(&filename).map_err(|err| err.to_string())?;
        use Message::*;
        thread::spawn(move || -> Result<(), String> {
            let mut prev_consumer = i as usize;
            while let Ok(Produce(mut cfg, mut buffer)) = rx.recv() {
                let chunk_size = if i != num_producers - 1 {
                    task_chunk_size.min(end_offset - offset)
                } else {
                    last_prod_task_chunk_size.min(end_offset - offset)
                };
                //println!(">> cfg chunk size: {}", cfg.chunk_size);
                //println!("buffer.capacity(): {} cfg.chunk_size: {}", buffer.capacity(), cfg.chunk_size);
                assert!(buffer.capacity() >= chunk_size as usize);
                unsafe {
                    buffer.set_len(chunk_size as usize);
                }
                //bf.seek(SeekFrom::Start(cfg.cur_offset)).unwrap();
                //file.seek(SeekFrom::Current(df as u64)).unwrap();
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

                //match bf.read_exact(&mut buffer) {
                match read_bytes_at(&mut buffer, &file, offset as u64) {
                    //}, &file, offset)//file.read_exact(&mut buffer) {
                    Err(err) => {
                        //panic!("offset: {} cur_offset: {} buffer.len: {}", cfg.offset, cfg.cur_offset, buffer.len());
                        return Err(format!("Error reading data: {}", err.to_string()));
                    }
                    Ok(()) => {
                        chunk_id += 1;
                        cfg.chunk_id = chunk_id;
                        cfg.offset = offset;
                        offset += buffer.len() as u64;
                        //println!("Sending message to consumer {}", c);
                        if let Err(err) = cfg.consumers[c].send(Consume(cfg.clone(), buffer)) {
                            return Err(format!("Error sending to consumer - {}", err.to_string()));
                        }
                        if offset as u64 >= end_offset {
                            // signal the end of stream to consumers
                            (0..cfg.consumers.len()).for_each(|x| {
                                //println!("{}>> Sending End of message to consumer {}", i, x);
                                let _ = cfg.consumers[x].send(End(i, num_producers));
                            });
                            break;
                        }
                    }
                }
            }
            //println!("Producer {} exiting", i);
            return Ok(());
        });
    }
    Ok(tx_producers)
}

// -----------------------------------------------------------------------------
/// Build consumers and return tuple of (Sender objects, JoinHandles)
fn build_consumers<T: 'static + Clone + Send, R: 'static + Clone + Sync + Send>(
    num_consumers: u64,
    f: Arc<Consumer<T, R>>,
    data: T,
) -> (Senders, Vec<JoinHandle<Vec<(u64, R)>>>) {
    let mut consumers_handles = Vec::new();
    let mut tx_consumers = Vec::new();
    for _i in 0..num_consumers {
        let (tx, rx) = channel();
        tx_consumers.push(tx);
        use Message::*;
        let cc = FnMove { f: f.clone() };
        let data = data.clone();
        let h = thread::spawn(move || {
            let mut ret = Vec::new();
            let mut producers_end_signal_count = 0;
            let mut _bytes = 0;
            loop {
                // consumers tx endpoints live inside the ReadData instance
                // sent along messages, when producers finish sending data
                // all transmission endpoints die resulting in recv()
                // failing and consumers exiting
                if let Ok(msg) = rx.recv() {
                    match msg {
                        Consume(cfg, buffer) => {
                            _bytes += buffer.len();
                            //println!("{}> Received {} bytes from [{}]", i, buffer.len(), cfg.producer_id);
                            ret.push((
                                cfg.chunk_id,
                                cc.call(&buffer, &data, cfg.chunk_id, cfg.num_chunks, cfg.offset),
                            ));
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
                        }
                        End(_prod_id, num_producers) => {
                            producers_end_signal_count += 1;
                            /*println!(
                                "{}> received End signal from {} {}/{}",
                                i, _prod_id, producers_end_signal_count, num_producers
                            );*/
                            if producers_end_signal_count >= num_producers {
                                //println!(
                                //    "{}>> {} {}/{}",
                                //    i, bytes, producers_end_signal_count, producers_per_consumer
                                //);
                                break;
                            }
                        }
                        _ => {
                            // this should be unreachable!
                            panic!("Wrong message type received");
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
            return ret;
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
    task_chunk_size: u64,
    last_producer_task_chunk_size: u64,
    chunks_per_producer: u64,
    reserved_size: usize,
    num_tasks_per_producer: u64,
) -> Result<(), String> {
    let num_producers = tx_producers.len() as u64;
    let num_buffers_per_producer = num_tasks_per_producer;
    for i in 0..num_producers {
        let tx = tx_producers[i as usize].clone();
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
                chunk_id: 0, //overwritten
                num_chunks: chunks_per_producer * num_producers,
                producer_tx: tx.clone(),
                consumers: tx_consumers.clone(),
                offset: 0, // overwritten
            };
            if let Err(err) = tx.send(Message::Produce(cfg, buffer)) {
                return Err(format!("Error sending to producer - {}", err.to_string()));
            }
        }
    }
    Ok(())
}
#[cfg(any(windows))]
fn read_bytes_at(buffer: &mut Vec<u8>, file: &File, offset: u64) -> Result<(), String> {
    use std::os::windows::fs::FileExt;
    let mut data_read = 0;
    while data_read < buffer.len() {
        data_read += file
            .seek_read(&mut buffer[data_read..], offset)
            .map_err(|err| err.to_string())?;
    }
}

#[cfg(any(unix))]
fn read_bytes_at(buffer: &mut Vec<u8>, file: &File, offset: u64) -> Result<(), String> {
    use std::os::unix::fs::FileExt;
    let mut data_read = 0;
    while data_read < buffer.len() {
        data_read += file
            .read_at(&mut buffer[data_read..], offset)
            .map_err(|err| err.to_string())?;
    }
    Ok(())
}
