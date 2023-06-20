use crate::WalError;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub(crate) struct WalWriter {
    // shared buffer
    buffer: Arc<Mutex<Vec<Vec<u8>>>>,
    // Handle to current file
    file: File,
    // storage capacity per file
    capacity_per_file: usize,
    // storage capacity filled in the current file
    filled: usize,
    // file sequence number for the current file
    pointer: u8,
}

impl WalWriter {
    pub fn new(location: &str, capacity: usize) -> Result<Self, WalError> {
        // let base_path = PathBuf::from(location);
        // let mut pointer_path = base_path.clone();
        // pointer_path.push("pointer");
        // if pointer_path.exists() {
        //     // load existing files
        // } else {
        //     // create a new file for writing logs
        //     let mut file = match File::create(file_path) {
        //         Ok(f) => f,
        //         Err(_) => {
        //             return Err(WalError::File("Failed to create pointer file".to_string()));
        //         }
        //     };
        //     // write current pointer
        //     if let Err(_) = file.write_all(content.as_bytes()) {
        //         return Err(WalError::File(
        //             "Failed to write to pointer file".to_string(),
        //         ));
        //     }
        //     // create the 5 buffer files
        //     for i in 1..6 {
        //         let file_name = format!("wal_{}", i);
        //         let mut path = base_path.clone();
        //         path.push(file_name);
        //     }
        // }

        todo!()
    }
}
