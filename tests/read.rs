use par_io;
use std::{fs::File, char::ToLowercase};


fn to_u8_slice<T>(v: &Vec<T>) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(
            v.as_ptr() as *const u8,
            v.len() * std::mem::size_of::<T>())
    }
}
#[derive(Clone)]
struct Dummy {}

#[test]
fn read() -> Result<(), String> {
    //1 create array
    let buf : Vec<u32> = (1_u32..=1111).collect();
    let bytes = to_u8_slice(&buf);   
    //the following should be optimised to noop
    //let buf: Vec<u8> = buf.iter().map(|x| x as u8).collect();
    //2 save to file
    let filename = "tmp-read_test";
    let mut file = File::options().write(true).open(filename).unwrap();
    use std::io::Write;
    file.write(bytes).unwrap();
    drop(file);
    //3 read file in parallel
    let consume =
    |buffer: &[u8], _data: &Dummy, _chunk_id: u64, _num_chunks: u64, offset: u64| -> (u64, Vec<u8>) {
        (offset, buffer.to_vec())
    };
    let num_producers = 4;
    let num_consumers = 2;
    let chunks_per_producer = 3;
    let num_buffers_per_producer = 2;
    let total_chunks = num_producers * chunks_per_producer;
    let mut map = std::collections::BTreeMap::new();
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
        use std::io::Seek;
        use std::io::SeekFrom;
        use std::io::Cursor;
        let mut out = Cursor::new(&mut b); 
        Ok(v) => {
                let w = v.iter()
                        .for_each(|(offset, v)| {out.seek(SeekFrom::Start(offset)); out.write(v);});
        },
        Err(err) => {
            return Err(err.to_string());
        }
    }
    assert_eq!(bytes, b);
    //5 verify result
    Ok(())
}

