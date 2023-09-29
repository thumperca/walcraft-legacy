use crate::{LogEntry, WalError};
use std::fs::OpenOptions;
use std::io::Read;
use std::path::PathBuf;

pub(crate) struct WalReader {
    location: PathBuf,
}

impl WalReader {
    pub fn new(location: PathBuf) -> Self {
        Self { location }
    }

    pub fn read(&self) -> Result<Vec<LogEntry>, WalError> {
        let pointer = self.current_pointer()?;
        let read_order = Self::read_order(pointer);
        let mut buffer = vec![];
        for i in read_order {
            {
                let file_name = format!("wal_{}", i);
                let mut path = self.location.clone();
                path.push(file_name);
                if let Ok(mut file) = OpenOptions::new().read(true).open(path) {
                    file.read_to_end(&mut buffer)
                        .map_err(|_| WalError::File("Failed to read file".to_string()))?;
                }
            }
        }
        let mut data = Vec::new();
        let mut offset = 0;
        while offset < buffer.len() {
            let bytes = [
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ];
            let size = u32::from_ne_bytes(bytes) as usize;
            let end = offset + 4 + size;
            let d = Vec::from(&buffer[offset + 4..end]);
            data.push(LogEntry::from_vec(d));
            offset = end;
        }
        Ok(data)
    }

    fn current_pointer(&self) -> Result<u8, WalError> {
        let mut path = self.location.clone();
        path.push("meta");
        let s = std::fs::read_to_string(path)
            .map_err(|_| WalError::File("Failed to read pointer file".to_string()))?;
        s.parse::<u8>()
            .map_err(|_| WalError::File("Failed to read pointer file".to_string()))
    }

    fn read_order(mut pointer: u8) -> Vec<u8> {
        let mut d = Vec::with_capacity(5);
        while d.len() < 5 {
            d.push(pointer);
            pointer -= 1;
            if pointer < 1 {
                pointer = 5;
            }
        }
        d
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let location = PathBuf::from("./tmp/");
        let reader = WalReader::new(location);
        let d = reader.read();
        println!("d is {:?}", d);
    }

    #[test]
    fn order() {
        assert_eq!(WalReader::read_order(5), Vec::from([5, 4, 3, 2, 1]));
        assert_eq!(WalReader::read_order(4), Vec::from([4, 3, 2, 1, 5]));
        assert_eq!(WalReader::read_order(3), Vec::from([3, 2, 1, 5, 4]));
        assert_eq!(WalReader::read_order(2), Vec::from([2, 1, 5, 4, 3]));
        assert_eq!(WalReader::read_order(1), Vec::from([1, 5, 4, 3, 2]));
    }
}
