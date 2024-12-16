use std::num::NonZero;
use std::ops::ControlFlow;
use std::path::Path;
use std::sync::mpsc;

/// Buffering of records.
mod buffer;
/// Program data model.
pub mod data;
/// Simple abstractions for working with the output file, both from writing and reading ends.
pub mod output;

pub use buffer::Config as BufferConfig;
use data::*;

pub type ReceiversTuple = (
    mpsc::Receiver<DataA>,
    mpsc::Receiver<DataB>,
    mpsc::Receiver<DataC>,
    mpsc::Receiver<DataD>,
    mpsc::Receiver<DataE>,
);

pub struct NewRecordsAvailable(pub NonZero<usize>);

pub struct UnsortedDataSinkLoop<'w, P> {
    pub receivers: ReceiversTuple,
    pub writer: &'w mut output::Writer,
    pub notify_new_records: mpsc::Sender<NewRecordsAvailable>,
    pub buffer_dir: P,
    pub buffer_config: BufferConfig,
}

impl<'w, P: AsRef<Path>> UnsortedDataSinkLoop<'w, P> {
    pub fn run(mut self) {
        std::thread::scope(|scope| {
            let (tx, rx) = mpsc::channel::<Record>();

            let tx1 = tx.clone();
            scope.spawn(move || channel_data_as_record(self.receivers.0, tx1));
            let tx1 = tx.clone();
            scope.spawn(move || channel_data_as_record(self.receivers.1, tx1));
            let tx1 = tx.clone();
            scope.spawn(move || channel_data_as_record(self.receivers.2, tx1));
            let tx1 = tx.clone();
            scope.spawn(move || channel_data_as_record(self.receivers.3, tx1));
            scope.spawn(move || channel_data_as_record(self.receivers.4, tx));

            let mut buffer =
                buffer::Buffer::new(&self.buffer_dir, &mut self.writer, self.buffer_config);
            let mut last_timestamps: [Option<Timestamp>; 5] = [None; 5];

            while let Ok(record) = rx.recv() {
                let idx = match record {
                    Record::A(_) => 0,
                    Record::B(_) => 1,
                    Record::C(_) => 2,
                    Record::D(_) => 3,
                    Record::E(_) => 4,
                };
                last_timestamps[idx] = Some(record.timestamp());

                buffer.push_record(record);

                if let Some(ts) = find_earliest_timestamp(last_timestamps.into_iter()) {
                    let buffer::DumpedCount(count) = buffer.try_dump(ts);
                    if let Some(count) = NonZero::new(count) {
                        if let Err(_) = self.notify_new_records.send(NewRecordsAvailable(count)) {
                            break;
                        };
                    }
                }
            }
        });
    }
}

fn find_earliest_timestamp(
    mut items: impl Iterator<Item = Option<Timestamp>>,
) -> Option<Timestamp> {
    if let ControlFlow::Continue(Some(value)) = items.try_fold(None, |acc, item| match item {
        None => ControlFlow::Break(None::<Timestamp>),
        Some(x) => ControlFlow::Continue(Some(acc.map_or(x, |y| std::cmp::min(x, y)))),
    }) {
        Some(value)
    } else {
        None
    }
}

fn channel_data_as_record<T: Into<Record>>(rx: mpsc::Receiver<T>, tx: mpsc::Sender<Record>) {
    while let Ok(data) = rx.recv() {
        if let Err(_) = tx.send(data.into()) {
            break;
        }
    }
}

pub struct SortedOutputListenLoop<'r> {
    reader: &'r mut output::Reader,
    notify_new_records: mpsc::Receiver<NewRecordsAvailable>,
}

impl<'r> SortedOutputListenLoop<'r> {
    pub fn run(self) {
        while let Ok(NewRecordsAvailable(count)) = self.notify_new_records.recv() {
            println!("reading next {count} records, ensuring their proper order");
            let mut prev = self
                .reader
                .read()
                .expect("must be available, count is non-zero")
                .timestamp();
            for _ in 1..count.get() {
                let record = self.reader.read().expect("must be available");

                let ts = record.timestamp();
                assert!(ts >= prev);
                prev = ts;
            }
            println!("checked all written records!");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_min_timestamp() {
        let items = [None, Some(Timestamp(45))];
        assert_eq!(find_earliest_timestamp(items.into_iter()), None);

        let items = [Some(Timestamp(0)), None, Some(Timestamp(45))];
        assert_eq!(find_earliest_timestamp(items.into_iter()), None);

        let items = [
            Some(Timestamp(100)),
            Some(Timestamp(5)),
            Some(Timestamp(45)),
        ];
        assert_eq!(
            find_earliest_timestamp(items.into_iter()),
            Some(Timestamp(5))
        );
    }
}
