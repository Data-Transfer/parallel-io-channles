use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::mpsc::Receiver;
use std::thread;
use std::sync::mpsc::channel;
use std::sync::Arc;

type Senders = Vec<Sender<Message>>;
type Buffer = Vec<u8>;
type Offset = u64;
#[derive(Clone)]
struct ReadData {
    offset: Offset,
    size: u64,
    tx: Sender<Message>,
    consumers: Senders
}
enum Message {
    Read(ReadData, Buffer),
    End
}
fn select_tx(i: usize, _c: usize, p: usize) -> usize {
   i % p 
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
    let size = 1024;
    let chunk_size = 256;
    let file = File::open(filename).expect("Cannot open file");
   
    let mut tx_producers: Senders = Senders::new();
    for i in 0..num_producers {
        let (tx, rx) = channel();
        tx_producers.push(tx);
        let mut file = file.try_clone().unwrap();
        use Message::*;
        thread::spawn(move || {
            while let Ok(Read(mut rd, mut buffer)) = rx.recv() {
                buffer.reserve(chunk_size);
                assert!(buffer.capacity() >= chunk_size);
                unsafe { 
                    buffer.set_len(chunk_size);
                }
                file.seek(SeekFrom::Start(rd.offset)).unwrap();
                let c = select_tx(i, num_consumers, num_producers);
                match file.read(&mut buffer) {
                    Ok(0) | Err(_) => {
                        rd.consumers[c].send(End)
                            .expect("Error sending buffer");
                        return;
                    }
                    Ok(s) => {
                        assert!(buffer.capacity() >= s);
                        unsafe {
                            buffer.set_len(s);
                        }
                        rd.offset += s as u64;
                        rd.consumers[c].send(Read(rd.clone(), buffer))
                            .expect("Cannot send buffer");
                    }
                }
            }
        });
    }/*
    let mut consumers_handles = Vec::new();
    for i in 0..2 {
        let rx = consumers[i].1;
        let h = thread::spawn(move || {
                    while let Ok(FileBuf::MemBuffer(buffer, offset, tx)) = rx.recv() {
                        if buffer.len() == 0 {
                            return;
                        }
                        let t = String::from_utf8_lossy(&buffer);
                        println!("{}", t);
                        if let Err(_) = tx.send(FileBuf::MemBuffer(buffer, offset, tx.clone())) {
                            return;
                        }
                    }
        });
        consumers_handles.push(h);
    }
    for h in consumers_handles {
        h.join();
    }*/
}
