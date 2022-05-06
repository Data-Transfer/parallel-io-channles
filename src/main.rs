use std::fs::File;
use std::io::Read;
use std::sync::mpsc;
use std::thread;

type Buffer = Vec<u8>;

enum FileBuf {
    MemBuffer(Buffer),
}

/// Separate file read from data consumption using a fixed amount of memory.
/// * thread 1 reads data from file and sends it to thread 2
/// * thread 2 consumes data and sends consumed buffer back to thread 1 so that
///   it can be reused
fn main() {
    let filename = std::env::args().nth(1).expect("Missing file name");
    let (tx1, rx1) = mpsc::channel::<FileBuf>();
    let (tx2, rx2) = mpsc::channel::<FileBuf>();
    let mut file = File::open(filename).expect("Cannot open file");
    for _ in 0..2 {
        tx1.send(FileBuf::MemBuffer(vec![0_u8; 128]))
            .expect("Error sending buffer");
    }
    let _read_file = thread::spawn(move || {
        while let Ok(FileBuf::MemBuffer(mut buffer)) = rx1.recv() {
            match file.read(&mut buffer) {
                Ok(0) => {
                    return;
                }
                Ok(s) => {
                    tx2.send(FileBuf::MemBuffer(buffer))
                        .expect("Cannot send buffer");
                }
                _ => {return;}
            }
        }
    });
    let print_values = thread::spawn(move || {
        while let Ok(FileBuf::MemBuffer(buffer)) = rx2.recv() {
            let t = String::from_utf8_lossy(&buffer);
            println!("{}", t);
            tx1.send(FileBuf::MemBuffer(buffer))
                .expect("Error sending buffer from consumer");
        }
    });
    print_values.join().expect("Error joining thread");
}
