use par_io;
use std::fs::File;
use std::io::Cursor;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

fn to_u8_slice<T>(v: &Vec<T>) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * std::mem::size_of::<T>())
    }
}

/*fn from_u8_slice<T: Sized>(v: &[u8]) -> &[T] {
    unsafe {
        std::slice::from_raw_parts(
            v.as_ptr() as *const T,
            v.len() / std::mem::size_of::<T>())
    }
}*/

#[derive(Clone)]
struct Dummy {}

struct DeleteFile(String);

impl std::ops::Drop for DeleteFile {
    fn drop(&mut self) {
        std::fs::remove_file(&self.0).unwrap();
    }
}

#[test]
fn read() -> Result<(), String> {
    //1 create array
    let buf: Vec<u32> = (0_u32..1111).collect();
    let bytes = to_u8_slice(&buf);
    //the following could be optimised
    //let buf: Vec<u8> = buf.iter().map(|x| x as u8).collect();
    //2 save to file
    let filename = "tmp-read_test";
    let mut file = File::options()
        .create(true)
        .write(true)
        .open(filename)
        .map_err(|err| err.to_string())?;
    file.write(bytes).map_err(|err| err.to_string())?;
    drop(file);
    let _delete_file_at_end = DeleteFile(filename.to_string());
    //3 read file in parallel
    let consume = |buffer: &[u8],
                   _data: &Dummy,
                   _chunk_id: u64,
                   _num_chunks: u64,
                   offset: u64|
     -> (u64, Vec<u8>) { (offset, buffer.to_vec()) };
    let num_producers = 4;
    let num_consumers = 2;
    let chunks_per_producer = 3;
    let num_buffers_per_producer = 2;
    let mut b: Vec<u8> = Vec::new();
    match par_io::par_read::read_file(
        filename,
        num_producers,
        num_consumers,
        chunks_per_producer,
        std::sync::Arc::new(consume),
        Dummy {},
        num_buffers_per_producer,
    ) {
        Ok(v) => {
            let mut out = Cursor::new(&mut b);
            for (_, (offset, x)) in &v {
                out.seek(SeekFrom::Start(*offset))
                    .map_err(|err| err.to_string())?;
                out.write(x).map_err(|err| err.to_string())?;
            }
        }
        Err(err) => {
            return Err(err.to_string());
        }
    }
    //4 verify result
    assert_eq!(bytes, b);
    Ok(())
}
