//! Parallel async file read.
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
pub struct Config {
    chunk_id: u64,
    num_chunks: u64,
    producer_tx: Sender<Message>,
    consumers: Senders,
    offset: u64,
}
pub type ProducerConfig = Config;
pub type ConsumerConfig = Config;
type ProducerId = u64;
type NumProducers = u64;
pub enum Message {
    //Error(String), // sent when producer encounters and error
    Consume(ConsumerConfig, Buffer), // sent to consumers
    Produce(ProducerConfig, Buffer), // sent to producers
    End(ProducerId, NumProducers),   // sent from producers to all consumers
                                     // to signal end of transmission
}

#[derive(Debug)]
pub enum ReadError {
    IO(std::io::Error),
    Send(std::sync::mpsc::SendError<Message>), 
    Other(String)
}

// Moving a generic Fn instance requires customization
type Consumer<T, R> = dyn Fn(
    &[u8], // data read from file
    &T,    // client data
    u64,   // chunk id
    u64,   // number of chunks
    u64,   // file offset (where data is read from)
) -> R;
struct FnMove<T, R> {
    f: Arc<Consumer<T, R>>,
}
impl<T, R> FnMove<T, R> {
    fn call(&self, buf: &[u8], d: &T, a: u64, b: u64, c: u64) -> R {
        (self.f)(buf, d, a, b, c)
    }
}
unsafe impl<T, R> Send for FnMove<T, R> {}

// -----------------------------------------------------------------------------
/// Select target consumer given current producer ID.
/// Current implementation is round robin: each producer sends to the next
fn select_tx(
    _i: usize,
    previous_consumer_id: usize,
    num_consumers: usize,
    _num_producers: usize,
) -> usize {
    (previous_consumer_id + 1) % num_consumers
}

// -----------------------------------------------------------------------------
/// Separate file reading from data consumption using the producer-consumer pattern
/// and a fixed number of pre-allocated buffers to achieve constant memory usage.
///
/// * thread *i* reads data and sends it to thread *j*
/// * thread *j* receives data and passes it to client callback object, then sends buffer back to thread *i*
///
/// The number of buffers equals the number of producers times the number of buffers per producer,
/// regardless of the number of chunks read.
///
/// ## Arguments
/// * `filename` - file to read
/// * `num_producers` - number of producers = number of producer threads
/// * `num_consumers` - number of consumers = number of consumer threads
/// * `chunks_per_producer` - number of chunks per producer = number of file read tasks per producer
/// * `consumer` - function to consume data
/// * `client_data` - data to be passed to consumer function
/// * `num_buffers_per_producer` - number of buffers per producer; these buffers are sent to consumers and reused
///
/// Callback signature:
///
/// ```ignore
///
///type Consumer<T, R> = dyn Fn(&[u8], // data read from file
///                             &T,    // client data
///                             u64,   // chunk id
///                             u64,   // number of chunks
///                             u64    // file offset (where data is read from)
///                            ) -> R;
/// ```

// -----------------------------------------------------------------------------
// Internal layout.
// ```ignore
// |||..........|.....||..........|.....||...>> ||...|.|||
//    <---1----><--2->                            <3><4>
//    <-------5------>                            <--6->
//    <-------------------------7---------------------->
// ```
// 1. task_chunk_size
// 2. last_task_chunk_size
// 3. last_producer_task_chunk_size
// 4. last_producer_last_task_chunk_size
// 5. producer_chunk_size
// 6. last_producer_chunk_size
// 7. total_size
pub fn read_file<T: 'static + Clone + Send, R: 'static + Clone + Sync + Send>(
    filename: &str,
    num_producers: u64,
    num_consumers: u64,
    chunks_per_producer: u64,
    consumer: Arc<Consumer<T, R>>,
    client_data: T,
    num_buffers_per_producer: u64,
) -> Result<Vec<(u64, R)>, ReadError> {
    let total_size = match std::fs::metadata(&filename) {
        Ok(m) => m.len(),
        Err(err) => {
            return Err(ReadError::IO(err));
        }
    };
    let producer_chunk_size = (total_size + num_producers - 1) / num_producers;
    let last_producer_chunk_size = total_size - (num_producers - 1) * producer_chunk_size;
    let task_chunk_size = (producer_chunk_size + chunks_per_producer - 1) / chunks_per_producer;
    let last_task_chunk_size = producer_chunk_size - (chunks_per_producer - 1) * task_chunk_size;
    let last_prod_task_chunk_size =
        (last_producer_chunk_size + chunks_per_producer - 1) / chunks_per_producer;
    let last_last_prod_task_chunk_size =
        last_producer_chunk_size - (chunks_per_producer - 1) * last_prod_task_chunk_size;

    let (tx_producers, prods) = build_producers(num_producers, chunks_per_producer, &filename)?;
    let (tx_consumers, consumers_handles) = build_consumers(num_consumers, consumer, client_data);
    let reserved_size = last_task_chunk_size
        .max(last_last_prod_task_chunk_size)
        .max(task_chunk_size);
    launch(
        tx_producers,
        tx_consumers,
        task_chunk_size,
        last_prod_task_chunk_size,
        chunks_per_producer,
        reserved_size as usize,
        num_buffers_per_producer,
    )?;
     
    let mut ret = Vec::new();
    for h in consumers_handles {
        match h.join() {
            Ok(chunks) => {
                ret.extend(chunks);
            }
            Err(err) => {
                return Err(ReadError::Other(format!("{:?}", err)));
            }
        }
    }
    for p in prods {
        match p.join() {
            Ok(_) => {},
            Err(err) => {
                return Err(ReadError::Other(format!("{:?}", err)));
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
) -> Result<(Senders, Vec<JoinHandle<Result<(), ReadError>>>), ReadError> {
    let total_size = std::fs::metadata(filename).map_err(|err| ReadError::IO(err))?
        .len();
    let mut tx_producers: Senders = Senders::new();
    let producer_chunk_size = (total_size + num_producers - 1) / num_producers;
    let last_producer_chunk_size = total_size - (num_producers - 1) * producer_chunk_size;
    let task_chunk_size = (producer_chunk_size + chunks_per_producer - 1) / chunks_per_producer;
    let last_prod_task_chunk_size =
        (last_producer_chunk_size + chunks_per_producer - 1) / chunks_per_producer;
    let mut producer_handles = Vec::new(); 
    // currently producers exit after sending data, and consumers try
    // to send data back to disconnected producers, ignoring the returned
    // send() error
    // another option is to have consumers return an 'Exit' signal when done
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
        let file = File::open(&filename).map_err(|err| ReadError::IO(err))?;
        use Message::*;
        let h = thread::spawn(move || -> Result<(), ReadError> {
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
                let num_consumers = cfg.consumers.len();
                // to support multiple consumers per producer we need to keep track of
                // the destination; by adding the element into a Set and notify all
                // of them when the producer exits
                let c = select_tx(
                    i as usize,
                    prev_consumer,
                    num_consumers,
                    num_producers as usize,
                );
                prev_consumer = c;
                #[cfg(feature = "print_ptr")]
                println!("{:?}", buffer.as_ptr());

                match read_bytes_at(&mut buffer, &file, offset as u64) {
                    Err(err) => {
                        // signal the end of stream to consumers
                        (0..cfg.consumers.len()).for_each(|x| {
                            let _ = cfg.consumers[x].send(End(i, num_producers));
                        });
                        return Err(ReadError::IO(err));
                    }
                    Ok(()) => {
                        chunk_id += 1;
                        cfg.chunk_id = chunk_id;
                        cfg.offset = offset;
                        offset += buffer.len() as u64;
                        if let Err(err) = cfg.consumers[c].send(Consume(cfg.clone(), buffer)) {
                            return Err(ReadError::Send(err));
                        }
                        if offset as u64 >= end_offset {
                            // signal the end of stream to consumers
                            (0..cfg.consumers.len()).for_each(|x| {
                                let _ = cfg.consumers[x].send(End(i, num_producers));
                            });
                            break;
                        }
                    }
                }
            }
            return Ok(());
        });
        producer_handles.push(h);
    }
    Ok((tx_producers, producer_handles))
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
                            ret.push((
                                cfg.chunk_id,
                                cc.call(&buffer, &data, cfg.chunk_id, cfg.num_chunks, cfg.offset),
                            ));
                            if let Err(_err) = cfg.producer_tx.send(Produce(cfg.clone(), buffer)) {
                                // senders might have already exited at this point after having added
                                // data to the queue
                                // from Rust docs
                                // "A send operation can only fail if the receiving end of a channel is disconnected, implying that the data could never be received"
                                // TBD
                                //break;
                            }
                        }
                        End(_prod_id, num_producers) => {
                            producers_end_signal_count += 1;
                            if producers_end_signal_count >= num_producers {
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
                    //break;
                }
            }
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
    num_buffers_per_producer: u64,
) -> Result<(), ReadError> {
    let num_producers = tx_producers.len() as u64;
    let num_buffers_per_producer = num_buffers_per_producer;
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
                return Err(ReadError::Send(err));
            }
        }
    }
    Ok(())
}

// -----------------------------------------------------------------------------
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
fn read_bytes_at(buffer: &mut Vec<u8>, file: &File, offset: u64) -> Result<(), std::io::Error> {
    use std::os::unix::fs::FileExt;
    let mut data_read = 0;
    while data_read < buffer.len() {
        data_read += file
            .read_at(&mut buffer[data_read..], offset)?;
            //.map_err(|err| err.to_string())?;
    }
    Ok(())
}
