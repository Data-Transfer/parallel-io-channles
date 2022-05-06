use std::fs::File;
use std::io::Read;
use std::io::SeekFrom;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::thread;

type Buffer = Vec<u8>;
type Offset = u64;
enum FileBuf {
    MemBuffer(Buffer, Offset, Sender<FileBuf>), //, Arc<Receiver<FileBuf>>),
}

/// Separate file read from data consumption using a fixed amount of memory.
/// * thread 1 reads data from file and sends it to thread 2
/// * thread 2 consumes data and sends consumed buffer back to thread 1 so that
///   it can be reused
/// The sender sends the buffer and a copy of the sender instance to be used
/// to return the buffer to the sender.
fn main() {
    let filename = std::env::args().nth(1).expect("Missing file name");
    let producers = (0..4).map(|_| channel::<FileBuf>()).collect::<Vec::<_>>();
    let consumers = (0..2).map(|_| channel::<FileBuf>()).collect::<Vec::<_>>();
    let mut file = File::open(filename).expect("Cannot open file");
    let chunk_size = 128;
    for _ in 0..2 {
        tx1.send(FileBuf::MemBuffer(vec![0_u8; 128], tx1.clone()))
            .expect("Error sending buffer");
    }
    for i in 0..4 {
    thread::spawn(move || {
        while let Ok(FileBuf::MemBuffer(mut buffer, offset, tx1)) = rx1.recv() {
            buffer.reserve(chunk_size);
            assert!(buffer.capacity() >= chunk_size);
            unsafe { 
                buffer.set_len(chunk_size);
            }
            file.seek(SeekFrom::Start(offset));
            match file.read(&mut buffer) {
                Ok(0) | Err(_) => {
                    tx2.send(FileBuf::MemBuffer(Vec::new(), tx1))
                        .expect("Error sending buffer");
                    return;
                }
                Ok(s) => {
                    assert!(buffer.capacity() >= s);
                    unsafe {
                        buffer.set_len(s);
                    }
                    tx2.send(FileBuf::MemBuffer(buffer, tx1.clone()))
                        .expect("Cannot send buffer");
                }
            }
        }
    });
}
    let consumers_handles = Vec::new();
    for i in 0..2 {
        let rx = consumers[i].1;;
        let h = thread::spawn(move || {
                    while let Ok(FileBuf::MemBuffer(buffer, tx)) = rx.recv() {
                        if buffer.len() == 0 {
                            return;
                        }
                        let t = String::from_utf8_lossy(&buffer);
                        println!("{}", t);
                        if let Err(_) = tx.send(FileBuf::MemBuffer(buffer, tx.clone())) {
                            return;
                        }
                    }
        });
        consumers_handles.push(h);
    }
    print_values.join().expect("Error joining thread");
}
