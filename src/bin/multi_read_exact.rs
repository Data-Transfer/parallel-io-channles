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
use std::thread;

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
fn select_tx(i: usize, c: usize, _p: usize) -> usize {
    i % c
}

/// Separate file read from data consumption using a fixed amount of memory.
/// * thread 1 reads data from file and sends it to thread 2
/// * thread 2 consumes data and sends consumed buffer back to thread 1 so that
///   it can be reused
/// The sender sends the buffer and a copy of the sender instance to be used
/// to return the buffer to he sender. This way only the number of buffers equals
/// the number of producers regardless of the number chunks read.
/// Current requirement is that:
/// * *number of producers* >= *number of consumers*
/// * number of
fn main() {
    let filename = std::env::args().nth(1).expect("Missing file name");
    let num_producers = 4;
    let num_consumers = 2;
    let len = std::fs::metadata(&filename)
        .expect("Error reading file size")
        .len();
    let chunks_per_task = 3;
    let task_chunk_size = (len + num_producers - 1) / num_producers;
    let chunk_size = (task_chunk_size + chunks_per_task - 1) / chunks_per_task;

    println!(
        "File size: {}, Thread chunk size {},  Task chunk size: {}",
        len, task_chunk_size, chunk_size
    );
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
                    Ok(s) => {
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
    let mut tx_consumers: Senders = Senders::new();
    let mut consumers_handles = Vec::new();
    for i in 0..num_consumers {
        let (tx, rx) = channel();
        tx_consumers.push(tx);
        use Message::*;
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
                            //consume(&buffer);
                            println!("{}> {} {}/{}", i, bytes, consumers, consumers_per_producer);
                            //println!("{} Sending message to producer", i);
                            if let Err(err) = rd.producer_tx.send(Read(rd.clone(), buffer)) {
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

    let mut bytes_consumed = 0;
    for h in consumers_handles {
        bytes_consumed += h.join().expect("Error joining threads");
    }
    assert_eq!(bytes_consumed, len as usize);
}

fn consume(buffer: &Buffer) {
    let s = String::from_utf8_lossy(buffer);
}

//fn create_consumers<F: Fn(&[u8])->()>(f: F) -> Vec<Sender<Message>> {
//
//}
//
//fn file_read(file_name: &str,
//             num_producers: u32,
//             tx_consumers: Vec<Sender<Message>>,
//             chunks_per_producer: u32) {
//
//
//}
