use crate::data::*;
use crate::output;
use std::path::{Path, PathBuf};

mod in_memory {
    use super::on_disk::FileStorage;
    use super::*;
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    /// In-memory heap of records.
    ///
    /// It is a simple wrapper around [`BinaryHeap`] with domain knowledge. Records are pushed into the
    /// heap, and then popped in a sorted manner onto the disk with [`Buffer::drain_into_file`].
    #[derive(Debug)]
    pub struct Buffer {
        heap: BinaryHeap<Reverse<Record>>,
    }

    impl Buffer {
        /// Create with capacity
        pub fn with_capacity(capacity: usize) -> Self {
            Self {
                heap: BinaryHeap::with_capacity(capacity),
            }
        }

        /// Get the current number of records in memory.
        pub fn len(&self) -> usize {
            self.heap.len()
        }

        pub fn is_full(&self) -> bool {
            self.heap.len() == self.heap.capacity()
        }

        /// Push a record.
        pub fn push(&mut self, record: Record) {
            debug_assert!(self.heap.len() < self.heap.capacity());
            self.heap.push(Reverse(record));
        }

        /// Write all records from memory on the disk in sorted order.
        ///
        /// Returns [`None`] if there are no records.
        ///
        /// Empties the in-memory buffer.
        pub fn drain_into_file(&mut self, file: impl AsRef<Path>) -> Option<FileStorage> {
            FileStorage::new(&mut self.heap, file)
        }
    }
}

mod on_disk {
    use super::*;

    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    use std::fs::{File, OpenOptions};
    use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom};
    use std::num::NonZero;

    /// On-disk storage of records.
    ///
    /// Stored records are sorted (by timestamp). To implement merge-sort using multiple [`FileStorage`]
    /// buffers, [`FileStorage::earliest`] and [`FileStorage::drop_one`] could be used.
    ///
    /// It reads data in predefined small chunks, allowing to have a multitude of [`FileStorage`] buffers and
    /// to implement merge-sort efficiently in terms of RAM.
    ///
    /// TODO: buffer capacity is now fixed, which could cause problems if the amount of buffers is too large.
    ///     A dynamic capacity could be used in this case, which I am not going to implement here.
    #[derive(Debug)]
    pub struct FileStorage {
        file: Option<File>,
        // buffer: BufReader<File>,
        // last: Record,
        remaining: usize,
    }

    impl FileStorage {
        pub fn new(heap: &mut BinaryHeap<Reverse<Record>>, file: impl AsRef<Path>) -> Option<Self> {
            let Some(non_zero_len) = NonZero::new(heap.len()) else {
                return None;
            };

            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(file)
                .expect("should open");

            let mut writer = BufWriter::new(file);

            while let Some(item) = heap.pop() {
                bincode::serialize_into(&mut writer, &item).expect("should serialize");
            }

            // writer.flush().expect("should flush");
            let mut file = writer.into_inner().unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();

            Some(Self {
                file: Some(file),
                remaining: non_zero_len.get(),
            })
        }

        pub fn read(self, capacity: usize) -> FileStorageReader {
            FileStorageReader::new(self, capacity)
        }

        pub fn is_empty(&self) -> bool {
            self.remaining == 0
        }
    }

    #[derive(Debug)]
    pub struct FileStorageReader {
        storage: FileStorage,
        buffer: WrappedBufReader<File>,
        last: Option<LastRead>,
    }

    #[derive(Debug)]
    struct LastRead {
        record: Record,
        bytes_read: usize,
    }

    impl FileStorageReader {
        fn new(mut storage: FileStorage, capacity: usize) -> Self {
            let mut file = storage
                .file
                .take()
                .expect("this method is only called when there is some file");
            let bytes_read = file
                .seek(SeekFrom::Current(0))
                .expect("zero seeking could fail?") as usize;
            let buf_reader = BufReader::with_capacity(capacity, file);
            let mut reader = Self {
                storage,
                buffer: WrappedBufReader {
                    buf_reader,
                    bytes_read,
                },
                last: None,
            };
            reader.read_next();
            reader
        }

        pub fn last(&self) -> Option<&Record> {
            self.last.as_ref().map(|x| &x.record)
        }

        pub fn read_next(&mut self) {
            if self.last.is_some() {
                self.storage.remaining -= 1;
            }

            self.last = if !self.storage.is_empty() {
                let bytes_before = self.buffer.bytes_read;
                let record = bincode::deserialize_from(&mut self.buffer).expect(
                    "the file is not yet read fully, and it has a sequence of valid serialised records",
                );
                let bytes_read = self.buffer.bytes_read - bytes_before;
                Some(LastRead { record, bytes_read })
            } else {
                None
            }
        }

        pub fn close(mut self) -> FileStorage {
            let mut file = self.buffer.buf_reader.into_inner();
            file.seek(SeekFrom::Start(
                self.last.map_or(self.buffer.bytes_read, |x| {
                    self.buffer.bytes_read - x.bytes_read
                }) as u64,
            ))
            .expect("shouldn't fail?");
            self.storage.file = Some(file);
            self.storage
        }
    }

    #[derive(Debug)]
    struct WrappedBufReader<T> {
        bytes_read: usize,
        buf_reader: BufReader<T>,
    }

    impl<T: Read> Read for WrappedBufReader<T> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let bytes = self.buf_reader.read(buf)?;
            self.bytes_read += bytes;
            Ok(bytes)
        }
    }
}

/// [`Buffer`] configuration
pub struct Config {
    /// Number of records is allowed to store in memory
    pub max_in_memory: usize,
    /// Buffer capacity for reading from each file buffer, i.e. merge-sort buffer capacity
    pub file_read_buf_capacity: usize,
}

#[derive(Debug)]
pub(crate) struct Buffer<'w> {
    in_memory: in_memory::Buffer,
    files: Vec<on_disk::FileStorage>,
    files_counter: usize,
    files_dir: PathBuf,
    earliest_buffered_timestamp: Option<Timestamp>,
    output: &'w mut output::Writer,
}

impl<'w> Buffer<'w> {
    pub fn new(
        files_dir: impl AsRef<Path>,
        output: &'w mut output::Writer,
        config: Config,
    ) -> Self {
        Self {
            in_memory: in_memory::Buffer::with_capacity(config.max_in_memory),
            files: vec![],
            files_counter: 0,
            files_dir: files_dir.as_ref().to_path_buf(),
            earliest_buffered_timestamp: None,
            output,
        }
    }

    pub fn push_record(&mut self, record: Record) {
        let ts = record.timestamp();
        self.earliest_buffered_timestamp.replace(
            self.earliest_buffered_timestamp
                .map_or(ts, |prev| if ts < prev { ts } else { prev }),
        );

        self.in_memory.push(record);
        if self.in_memory.is_full() {
            self.dump_in_memory();
        }
    }

    fn dump_in_memory(&mut self) {
        // FIXME not nice code
        if self.in_memory.len() == 0 {
            return;
        };
        let id = self.files_counter;
        self.files_counter += 1;
        eprintln!("dumping in-memory (#{id})");
        let file = self
            .in_memory
            .drain_into_file(self.files_dir.join(format!("dump-{id}")))
            .expect("in-memory isn't empty");
        self.files.push(file);
    }

    pub fn try_dump(&mut self, safe_to_dump_timestamp: Timestamp) -> DumpedCount {
        let has_something_to_dump = self
            .earliest_buffered_timestamp
            .map(|ts| ts <= safe_to_dump_timestamp)
            .unwrap_or(false);
        if !has_something_to_dump {
            return DumpedCount(0);
        };

        // we will perform merge-sort only with files
        // FIXME: avoid this and use in-memory buffer alongside with file buffers
        self.dump_in_memory();

        let mut dumped = 0;

        // merge sort
        let mut readers: Vec<_> = self
            .files
            .drain(0..)
            // FIXME: document why
            .map(|x| x.read(8_192))
            .collect();
        loop {
            let reader_with_earliest_timestamp = readers
                .iter_mut()
                .filter_map(|x| {
                    let ts = x.last().map(|y| y.timestamp());
                    ts.map(|ts| (x, ts))
                })
                .min_by_key(|(_, ts)| *ts)
                .map(|(reader, _)| reader);

            if let Some(reader) = reader_with_earliest_timestamp {
                let record = reader.last().expect("must be due to filtering");
                if record.timestamp() > safe_to_dump_timestamp {
                    // we can no longer proceed with the merge sort
                    self.earliest_buffered_timestamp = Some(record.timestamp());
                    break;
                }

                // dump the record
                // FIXME unwrap
                self.output.write(&record).unwrap();
                reader.read_next();
                dumped += 1;
            } else {
                // all readers are empty
                self.earliest_buffered_timestamp = None;
                break;
            }
        }
        // FIXME unwrap
        self.output.flush().unwrap();

        // close the readers
        self.files.extend(readers.into_iter().filter_map(|reader| {
            let file = reader.close();
            if file.is_empty() {
                None
            } else {
                Some(file)
            }
        }));

        DumpedCount(dumped)
    }
}

pub(crate) struct DumpedCount(pub usize);

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::IteratorRandom;
    use rand::thread_rng;

    #[cfg(test)]
    mod tests {
        use super::*;

        fn in_memory_factory() -> in_memory::Buffer {
            let mut buffer = in_memory::Buffer::with_capacity(256);

            buffer.push(Record::A(DataA {
                timestamp: Timestamp(5),
                foo: "foo".to_string(),
            }));
            buffer.push(Record::C(DataC {
                timestamp: Timestamp(2),
                baz: (1, 2),
            }));
            buffer.push(Record::E(DataE {
                timestamp: Timestamp(10),
                def: vec![3, 1, 2],
            }));

            buffer
        }

        #[test]
        fn dump_in_memory_and_read_from_disk() {
            let mut in_memory = in_memory_factory();
            let file = tempfile::NamedTempFile::new().unwrap();

            let file = in_memory
                .drain_into_file(file.path())
                .expect("in-memory isn't empty");
            let mut reader = file.read(8_192);

            assert_eq!(in_memory.len(), 0);
            assert_eq!(reader.last().unwrap().timestamp(), Timestamp(2));

            reader.read_next();
            assert_eq!(reader.last().unwrap().timestamp(), Timestamp(5));

            reader.read_next();
            assert_eq!(reader.last().unwrap().timestamp(), Timestamp(10));

            reader.read_next();
            assert!(reader.last().is_none());

            let file = reader.close();
            assert!(file.is_empty());
        }

        #[test]
        fn reading_same_record_from_disk_repeatedly() {
            let mut in_memory = in_memory_factory();
            let file = tempfile::NamedTempFile::new().unwrap();

            let mut file = in_memory
                .drain_into_file(file.path())
                .expect("in-memory isn't empty");

            for _ in 0..5 {
                let reader = file.read(8_192);
                assert_eq!(
                    reader
                        .last()
                        .expect("we never call `read_next`")
                        .timestamp(),
                    Timestamp(2)
                );
                file = reader.close();
            }

            let mut reader = file.read(8_192);
            reader.read_next();
            reader.read_next();
            reader.read_next();
            assert!(reader.last().is_none());
        }
    }

    #[test]
    fn process_a_few_records_in_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("output");
        let mut writer = output::Writer::open(&output).unwrap();
        let mut reader = output::Reader::open(&output).unwrap();
        let mut sut = Buffer::new(
            dir.path(),
            &mut writer,
            Config {
                max_in_memory: 10,
                file_read_buf_capacity: 8_192,
            },
        );

        sut.push_record(Record::A(DataA {
            timestamp: Timestamp(5),
            foo: "foo".to_owned(),
        }));
        sut.push_record(Record::A(DataA {
            timestamp: Timestamp(1),
            foo: "foo".to_owned(),
        }));
        sut.push_record(Record::A(DataA {
            timestamp: Timestamp(3),
            foo: "foo".to_owned(),
        }));

        let DumpedCount(count) = sut.try_dump(Timestamp(10));
        assert_eq!(count, 3);

        assert_eq!(reader.read().unwrap().timestamp(), Timestamp(1));
        assert_eq!(reader.read().unwrap().timestamp(), Timestamp(3));
        assert_eq!(reader.read().unwrap().timestamp(), Timestamp(5));
        let _ = reader.read().unwrap_err();
    }

    #[test]
    fn random_million_records_is_sorted() {
        const RECORDS: usize = 1_000_000;

        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("output");
        let mut writer = output::Writer::open(&output).unwrap();
        let mut sut = Buffer::new(
            dir.path(),
            &mut writer,
            Config {
                max_in_memory: 100_000,
                file_read_buf_capacity: 8_192,
            },
        );

        for _ in 0..RECORDS {
            let record = Record::E(DataE {
                timestamp: Timestamp(
                    (0..RECORDS as u128)
                        .choose(&mut thread_rng())
                        .expect("there is a plenty of choice"),
                ),
                def: vec![],
            });
            sut.push_record(record);
        }

        let count = sut.try_dump(Timestamp(RECORDS as u128));
        assert_eq!(count.0, RECORDS);

        let mut reader = output::Reader::open(&output).unwrap();
        let mut prev_ts = reader.read().unwrap().timestamp();
        for _ in 1..RECORDS {
            let ts = reader.read().unwrap().timestamp();
            assert!(prev_ts <= ts);
            prev_ts = ts;
        }
        reader.read().expect_err("there must be no records left");
    }
}
