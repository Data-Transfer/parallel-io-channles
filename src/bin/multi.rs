use std::fs::File;
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
    size: usize,
    producer_tx: Sender<Message>,
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
/// to return the buffer to he sender.
fn main() {
    let filename = std::env::args().nth(1).expect("Missing file name");
    let num_producers = 4;
    let num_consumers = 2;
    let file = File::open(filename).expect("Cannot open file");
    let len = file.metadata().unwrap().len();
    let size = len / num_producers;
    let num_chunks = 1;
    let chunk_size = (size / num_chunks) as usize;

    let mut tx_producers: Senders = Senders::new();
    for i in 0..num_producers {
        let (tx, rx) = channel();
        tx_producers.push(tx);
        let mut file = file.try_clone().unwrap();
        use Message::*;
        thread::spawn(move || {
            while let Ok(Read(mut rd, mut buffer)) = rx.recv() {
                buffer.reserve(rd.size);
                assert!(buffer.capacity() >= chunk_size);
                unsafe {
                    buffer.set_len(chunk_size);
                }
                file.seek(SeekFrom::Start(rd.offset)).unwrap();
                let c = select_tx(i as usize, num_consumers, num_producers as usize);
                match file.read(&mut buffer) {
                    Ok(0) | Err(_) => {
                        rd.consumers.iter().for_each(|c| {
                            let _ = c.send(End);
                        });
                        return;
                    }
                    Ok(s) => {
                        assert!(buffer.capacity() >= s);
                        unsafe {
                            buffer.set_len(s);
                        }
                        rd.offset += s as u64;
                        rd.consumers[c]
                            .send(Read(rd.clone(), buffer))
                            .expect(&format!("Cannot send buffer {}", s));
                    }
                }
            }
        });
    }
    let mut tx_consumers: Senders = Senders::new();
    let mut consumers_handles = Vec::new();
    for _ in 0..num_consumers {
        let (tx, rx) = channel();
        tx_consumers.push(tx);
        use Message::*;
        let h = thread::spawn(move || loop {
            if let Ok(Read(rd, buffer)) = rx.recv() {
                consume(&buffer);
                rd.producer_tx.send(Read(rd.clone(), buffer)).expect("Error sending");
            } else {
                return;
            }
        });
        consumers_handles.push(h);
    }

    for i  in 0..num_producers {
        let tx = tx_producers[i as usize].clone();
        let buffer = vec![0_u8; chunk_size];
        let offset = i * chunk_size as u64;
        let rd = ReadData {
            offset: offset,
            size: chunk_size,
            producer_tx: tx.clone(),
            consumers: tx_consumers.clone()
        };
        tx.send(Message::Read(rd, buffer)).expect("Cannot send");
    }


    for h in consumers_handles {
        h.join().expect("Error joining threads");
    }
}

fn consume(buffer: &Buffer) {
    let s = String::from_utf8_lossy(buffer);
    println!("{}", s);
}
