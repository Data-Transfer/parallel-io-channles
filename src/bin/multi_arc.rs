/// @todo: Handle non divisible buffer length
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::sync::Arc;
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
    consumers: Senders,
}
enum Message {
    Read(ReadData, Buffer),
    //End,
}
fn select_tx(i: usize, c: usize, _p: usize) -> usize {
    i % c
}

/// Separate file read from data consumption using a fixed amount of memory.
/// * thread 1 reads data from file and sends it to thread 2
/// * thread 2 consumes data and sends consumed buffer back to thread 1 so that
///   it can be reused
/// The sender sends the buffer and a copy of the sender instance to be used
/// to return the buffer to he sender.
fn main() {
    let filename = std::env::args().nth(1).expect("Missing file name");
    let num_producers = 1; //4;
    let num_consumers = 1; //2;
    let file = File::open(filename).expect("Cannot open file");
    let len = file.metadata().unwrap().len();
    let size = len / num_producers;
    let num_chunks = 1;
    let chunk_size = (size / num_chunks) as usize;

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
        let mut file = file.try_clone().unwrap();
        use Message::*;
        thread::spawn(move || {
            while let Ok(Read(mut rd, mut buffer)) = rx.recv() {
                //let mut buffer = Arc::get_mut(&mut buf).unwrap();
                // this should never happen
                if rd.cur_offset - rd.offset >= rd.size as u64 {
                    break;
                }
                println!("** [{}] {:?}", i, buffer.as_ptr());
                //buffer.reserve(rd.chunk_size);
                //println!("** [{}] {:?}", i, buffer.as_ptr());
                assert!(buffer.capacity() >= rd.chunk_size);
                unsafe {
                    buffer.set_len(rd.chunk_size);
                }
                println!("** [{}] {:?}", i, buffer.as_ptr());
                file.seek(SeekFrom::Start(rd.cur_offset)).unwrap();
                let c = select_tx(i as usize, num_consumers, num_producers as usize);
                match file.read(&mut buffer) {
                    Err(err) => {
                        panic!("{}", err.to_string());
                    }
                    Ok(s) => {
                        println!("Sending {} bytes to {}", s, c);
                        if s == 0 {
                            return;
                        }
                        assert!(buffer.capacity() >= s);
                        unsafe {
                            buffer.set_len(s);
                        }
                        rd.cur_offset += s as u64;
                        rd.consumers[c]
                            .send(Read(rd.clone(), buffer))
                            .expect(&format!("Cannot send buffer {}", s));
                        if rd.cur_offset - rd.offset >= rd.size as u64 {
                            break;
                        }
                    }
                }
            }
            println!("producer {} exiting", i);
            return;
        });
    }
    let mut tx_consumers: Senders = Senders::new();
    let mut consumers_handles = Vec::new();
    for i in 0..num_consumers {
        let (tx, rx) = channel();
        tx_consumers.push(tx);
        let received_size = len / num_consumers as u64;
        use Message::*;
        let h = thread::spawn(move || loop {
            let mut received = 0;
            loop {
                if let Ok(msg) = rx.recv() {
                    match msg {
                        Read(rd, buffer) => {
                            println!("> {:?}", buffer.as_ptr());
                            //println!("cur offset: {}, offset: {} size: {}, cur >= offset: {}", rd.cur_offset, rd.offset, rd.size,  rd.cur_offset - rd.offset >= rd.size as u64);
                            //consume(&buffer);
                            received += buffer.len();
                            println!("{} {}/{}", i, received, received_size);
                            if received >= received_size as usize {
                                break;
                            }
                            if let Err(err) = rd.producer_tx.send(Read(rd.clone(), buffer)) {
                                // senders might have already exited at this point after having added
                                // data to the queue
                                // from Rust docs
                                //A send operation can only fail if the receiving end of a channel is disconnected, implying that the data could never be receive
                                // TBD
                            }
                        }
                        _ => {
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
            println!("consumer {} exiting", i);
            return;
        });
        consumers_handles.push(h);
    }

    for i in 0..num_producers {
        let tx = tx_producers[i as usize].clone();
        let buffer = vec![0_u8; chunk_size];
        let offset = i * chunk_size as u64;
        let rd = ReadData {
            offset: offset,
            cur_offset: offset,
            size: chunk_size,
            chunk_size: chunk_size / 2,
            producer_tx: tx.clone(),
            consumers: tx_consumers.clone(),
        };
        tx.send(Message::Read(rd, buffer)).expect("Cannot send");
    }

    for h in consumers_handles {
        h.join().expect("Error joining threads");
    }
    println!("Finished!");
}

fn consume(buffer: &Buffer) {
    let s = String::from_utf8_lossy(buffer);
    println!("{}", s);
}
