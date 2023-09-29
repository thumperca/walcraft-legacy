use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct LogEntry {
    inner: Vec<u8>,
    // checksum: u32 <- for future usage - Todo
}

impl LogEntry {
    pub fn new<T>(data: T) -> Option<LogEntry>
    where
        T: Serialize + for<'a> Deserialize<'a>,
    {
        let encoded = match bincode::serialize(&data) {
            Ok(d) => d,
            Err(_) => {
                return None;
            }
        };
        Some(Self { inner: encoded })
    }

    pub fn from_vec(v: Vec<u8>) -> Self {
        Self { inner: v }
    }

    pub fn to_vec(self) -> Vec<u8> {
        let size: [u8; 4] = (self.inner.len() as u32).to_ne_bytes();
        let mut out = Vec::from(size);
        out.extend(self.inner.into_iter());
        out
    }

    pub fn to_original<T>(self) -> Option<T>
    where
        T: Serialize + for<'a> Deserialize<'a>,
    {
        bincode::deserialize(&self.inner).ok()
    }
}
