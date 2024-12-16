use rand::prelude::IteratorRandom;
use rand::{random, thread_rng};
use std::ops::Range;
use std::sync::mpsc;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tsk1183::{data::*, *};

fn main() {
    std::thread::scope(|scope| {
        let dir = tempdir().unwrap();
        let output_path = dir.path().join("output");
        let writer = output::Writer::open(&output_path).unwrap();
        let mut reader = output::Reader::open(&output_path).unwrap();

        let channels = (
            mpsc::channel(),
            mpsc::channel(),
            mpsc::channel(),
            mpsc::channel(),
            mpsc::channel(),
        );
        let notify_new_records = mpsc::channel();

        scope.spawn(move || {
            produce_loop((
                channels.0 .0,
                channels.1 .0,
                channels.2 .0,
                channels.3 .0,
                channels.4 .0,
            ));
        });

        scope.spawn(move || {
            // moving
            let mut writer = writer;
            UnsortedDataSinkLoop {
                receivers: (
                    channels.0 .1,
                    channels.1 .1,
                    channels.2 .1,
                    channels.3 .1,
                    channels.4 .1,
                ),
                writer: &mut writer,
                notify_new_records: notify_new_records.0,
                buffer_dir: dir.path(),
                buffer_config: BufferConfig {
                    max_in_memory: 1000,
                    file_read_buf_capacity: 8_192,
                },
            }
            .run()
        });

        consume_loop(&mut reader, notify_new_records.1);
    })
}

fn produce_loop(
    senders: (
        mpsc::Sender<DataA>,
        mpsc::Sender<DataB>,
        mpsc::Sender<DataC>,
        mpsc::Sender<DataD>,
        mpsc::Sender<DataE>,
    ),
) {
    const TIME_ERROR: Duration = Duration::from_secs(10);
    const TICK: Duration = Duration::from_millis(50);
    const DATA_PER_TICK: Range<u128> = 5..20;

    struct TimestampGen {
        start: Instant,
    }
    impl TimestampGen {
        fn timestamp(&self) -> Timestamp {
            let x = random::<f32>();
            let error =
                Duration::from_millis(((TIME_ERROR.as_millis() as f32) * (1.0 - x * 2.0)) as u64);
            Timestamp((self.start.elapsed() + error).as_millis())
        }
    }

    let timestamp_gen = TimestampGen {
        start: Instant::now(),
    };

    loop {
        let emit_count = DATA_PER_TICK
            .choose(&mut thread_rng())
            .expect("there is choice");

        for _ in 0..emit_count {
            let timestamp = timestamp_gen.timestamp();
            if match (0..5).choose(&mut thread_rng()).expect("there is choice") {
                0 => senders
                    .0
                    .send(DataA {
                        timestamp,
                        foo: "foo".to_string(),
                    })
                    .is_err(),
                1 => senders
                    .1
                    .send(DataB {
                        timestamp,
                        bar: false,
                    })
                    .is_err(),
                2 => senders
                    .2
                    .send(DataC {
                        timestamp,
                        baz: (0, 512),
                    })
                    .is_err(),
                3 => senders.3.send(DataD { timestamp, abc: () }).is_err(),
                4 => senders
                    .4
                    .send(DataE {
                        timestamp,
                        def: vec![5, 1, 2],
                    })
                    .is_err(),
                _ => unreachable!(),
            } {
                println!("some sender is dropped");
                break;
            };
        }

        println!("produced {emit_count} data in various channels");

        std::thread::sleep(TICK);
    }
}

fn consume_loop(
    reader: &mut output::Reader,
    notify_new_records: mpsc::Receiver<NewRecordsAvailable>,
) {
    while let Ok(NewRecordsAvailable(count)) = notify_new_records.recv() {
        println!("reading next {count} records, ensuring their proper order");
        let mut prev = reader
            .read()
            .expect("must be available, count is non-zero")
            .timestamp();
        for _ in 1..count.get() {
            let record = reader.read().expect("must be available");

            let ts = record.timestamp();
            assert!(ts >= prev);
            prev = ts;
        }
        println!("records order is fine!");
    }
}
