use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use walcraft::Wal;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Item {
    id: usize,
    value: String,
}

const SIZE: usize = 1_000_000;

fn generate_items() -> Vec<Item> {
    let mut data = Vec::with_capacity(SIZE);
    for i in 1..=SIZE {
        data.push(Item {
            id: i,
            value: format!("Id {}", i),
        });
    }
    data
}

fn main() {
    let wal1 = Wal::new("./tmp/", 1_000_000).unwrap();
    let wal2 = wal1.clone();

    let mut data = generate_items();
    let data2 = data.split_off(SIZE / 2);
    let start = Instant::now();

    let handle = std::thread::spawn(move || {
        for item in data {
            wal1.write(item);
            // std::thread::sleep(Duration::from_nanos(500));
        }
    });

    let handle2 = std::thread::spawn(move || {
        for item in data2 {
            wal2.write(item);
            // std::thread::sleep(Duration::from_nanos(500));
        }
    });

    handle.join().expect("t1 failed");
    handle2.join().expect("t2 failed");

    std::thread::sleep(Duration::from_secs(1));

    let end = start.elapsed();
    println!("addition took {:?}", end);
}
