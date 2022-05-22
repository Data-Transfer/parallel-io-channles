use std::sync::mpsc::channel;
use std::thread;

fn main() {
    let v = vec![0_u8; 1024];
    let (tx, rx) = channel::<Vec<u8>>();
    println!("{:?}", v.as_ptr());
    let h = thread::spawn(move || {
        let v = rx.recv().unwrap();
        println!("{:?}", v.as_ptr());
    });
    let h2 = thread::spawn(move || {
        tx.send(v).expect("Error sending");
    });
    h.join().unwrap();
    h2.join().unwrap();
}
