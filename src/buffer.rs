use crate::data::*;
use crate::output;
use std::path::{Path, PathBuf};

/// In-memory part of buffering
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
        pub fn drain_into_file(
            &mut self,
            file: impl AsRef<Path>,
        ) -> std::io::Result<Option<FileStorage>> {
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
    #[derive(Debug)]
    pub struct FileStorage {
        file: Option<File>,
        // buffer: BufReader<File>,
        // last: Record,
        remaining: usize,
    }

    impl FileStorage {
        /// Create by draining the heap into the file.
        ///
        /// Returns [`None`] if the heap is empty.
        ///
        /// TODO: make non-empty heap newtype?
        pub fn new(
            heap: &mut BinaryHeap<Reverse<Record>>,
            file: impl AsRef<Path>,
        ) -> std::io::Result<Option<Self>> {
            let Some(non_zero_len) = NonZero::new(heap.len()) else {
                return Ok(None);
            };

            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(file)?;

            let mut writer = BufWriter::new(file);

            while let Some(item) = heap.pop() {
                bincode::serialize_into(&mut writer, &item).map_err(unwrap_bincode_io_error)?;
            }

            let mut file = writer.into_inner().map_err(|err| err.into_error())?;
            file.seek(SeekFrom::Start(0))?;

            Ok(Some(Self {
                file: Some(file),
                remaining: non_zero_len.get(),
            }))
        }

        /// Create a reader
        pub fn read(self, capacity: usize) -> std::io::Result<FileStorageReader> {
            FileStorageReader::new(self, capacity)
        }

        pub fn is_empty(&self) -> bool {
            self.remaining == 0
        }
    }

    /// Performs reading from the file buffer in merge-sort-friendly way.
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
        fn new(mut storage: FileStorage, capacity: usize) -> std::io::Result<Self> {
            let mut file = storage
                .file
                .take()
                .expect("this method is only called when there is some file");
            let bytes_read =
                file.seek(SeekFrom::Current(0))
                    .expect("zero seeking couldn't fail, could it?") as usize;
            let buf_reader = BufReader::with_capacity(capacity, file);
            let mut reader = Self {
                storage,
                buffer: WrappedBufReader {
                    buf_reader,
                    bytes_read,
                },
                last: None,
            };
            reader.read_next()?;
            Ok(reader)
        }

        /// Last record in the file, i.e. the earliest in this file so far.
        ///
        /// [`Self::read_next`] moves to the next one (if there is).
        pub fn last(&self) -> Option<&Record> {
            self.last.as_ref().map(|x| &x.record)
        }

        /// Read the next record (if there is), changing the result of [`Self::last`]
        pub fn read_next(&mut self) -> std::io::Result<()> {
            if self.last.is_some() {
                self.storage.remaining -= 1;
            }

            self.last = if !self.storage.is_empty() {
                let bytes_before = self.buffer.bytes_read;
                let record =
                    bincode::deserialize_from(&mut self.buffer).map_err(unwrap_bincode_io_error)?;
                let bytes_read = self.buffer.bytes_read - bytes_before;
                Some(LastRead { record, bytes_read })
            } else {
                None
            };

            Ok(())
        }

        /// Close the reader. The next call to [`FileStorage::read`] will resume from the same
        /// position.
        pub fn close(mut self) -> std::io::Result<FileStorage> {
            let mut file = self.buffer.buf_reader.into_inner();
            file.seek(SeekFrom::Start(
                self.last.map_or(self.buffer.bytes_read, |x| {
                    self.buffer.bytes_read - x.bytes_read
                }) as u64,
            ))?;
            self.storage.file = Some(file);
            Ok(self.storage)
        }
    }

    /// Needed to track the exact number of bytes [`bincode`] reads.
    #[derive(Debug)]
    struct WrappedBufReader<T> {
        bytes_read: usize,
        buf_reader: BufReader<T>,
    }

    // TODO: implement more methods, forwarding them to the actual `BufReader`, for efficiency
    impl<T: Read> Read for WrappedBufReader<T> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let bytes = self.buf_reader.read(buf)?;
            self.bytes_read += bytes;
            Ok(bytes)
        }
    }
}

fn unwrap_bincode_io_error(err: Box<bincode::ErrorKind>) -> std::io::Error {
    match *err {
        bincode::ErrorKind::Io(err) => err,
        other => panic!("intentionally not covering serialisation errors in this task: {other}"),
    }
}

/// [`Buffer`] configuration
pub struct Config {
    /// Number of records is allowed to store in memory
    pub max_in_memory: usize,
    /// Buffer capacity for reading from each file buffer, i.e. merge-sort buffer capacity
    pub file_read_buf_capacity: usize,
}

/// _The_ buffer.
///
/// It accepts records via [`Buffer::push_record`], and dumps them based on the safe timestamp
/// with [`Buffer::dump_safe`].
#[derive(Debug)]
pub(crate) struct Buffer<'w> {
    in_memory: in_memory::Buffer,
    files: Vec<on_disk::FileStorage>,
    files_counter: usize,
    files_dir: PathBuf,
    file_read_buf_capacity: usize,
    earliest_buffered_timestamp: Option<Timestamp>,
    output: &'w mut output::Writer,
}

impl<'w> Buffer<'w> {
    pub fn new(
        files_dir: impl AsRef<Path>,
        output: &'w mut output::Writer,
        Config {
            max_in_memory,
            file_read_buf_capacity,
        }: Config,
    ) -> Self {
        Self {
            in_memory: in_memory::Buffer::with_capacity(max_in_memory),
            files: vec![],
            files_counter: 0,
            files_dir: files_dir.as_ref().to_path_buf(),
            file_read_buf_capacity,
            earliest_buffered_timestamp: None,
            output,
        }
    }

    /// Push a new record into the buffer.
    pub fn push_record(&mut self, record: Record) -> std::io::Result<()> {
        let ts = record.timestamp();
        self.earliest_buffered_timestamp.replace(
            self.earliest_buffered_timestamp
                .map_or(ts, |prev| if ts < prev { ts } else { prev }),
        );

        self.in_memory.push(record);
        if self.in_memory.is_full() {
            self.dump_in_memory()?;
        }

        Ok(())
    }

    fn dump_in_memory(&mut self) -> std::io::Result<()> {
        // FIXME not nice code

        if self.in_memory.len() == 0 {
            return Ok(());
        };
        let id = self.files_counter;
        self.files_counter += 1;
        eprintln!("dumping in-memory (#{id})");
        let file = self
            .in_memory
            .drain_into_file(self.files_dir.join(format!("dump-{id}")))?
            .expect("in-memory isn't empty");
        self.files.push(file);
        Ok(())
    }

    /// Dump the records that are safe to dump. It could as well be none!
    pub fn dump_safe(&mut self, safe_to_dump_timestamp: Timestamp) -> std::io::Result<DumpedCount> {
        let has_something_to_dump = self
            .earliest_buffered_timestamp
            .map(|ts| ts <= safe_to_dump_timestamp)
            .unwrap_or(false);
        if !has_something_to_dump {
            return Ok(DumpedCount(0));
        };

        // we will perform merge-sort only with files
        // FIXME: avoid this and use in-memory buffer alongside with file buffers
        self.dump_in_memory()?;

        let mut dumped = 0;

        // merge sort
        let mut readers: Vec<_> = self
            .files
            .drain(0..)
            .map(|x| x.read(self.file_read_buf_capacity))
            .collect::<Result<Vec<_>, _>>()?;
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
                self.output.write(&record)?;
                reader.read_next()?;
                dumped += 1;
            } else {
                // all readers are empty
                self.earliest_buffered_timestamp = None;
                break;
            }
        }
        self.output.flush()?;

        // close the readers
        self.files = readers
            .into_iter()
            .filter_map(|reader| match reader.close() {
                Err(err) => Some(Err(err)),
                Ok(file) if file.is_empty() => None,
                Ok(file) => Some(Ok(file)),
            })
            .collect::<Result<_, _>>()?;

        Ok(DumpedCount(dumped))
    }
}

/// The number of dumped records
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
        fn dump_in_memory_and_read_from_disk() -> std::io::Result<()> {
            let mut in_memory = in_memory_factory();
            let file = tempfile::NamedTempFile::new().unwrap();

            let file = in_memory
                .drain_into_file(file.path())?
                .expect("in-memory isn't empty");
            let mut reader = file.read(8_192)?;

            assert_eq!(in_memory.len(), 0);
            assert_eq!(reader.last().unwrap().timestamp(), Timestamp(2));

            reader.read_next()?;
            assert_eq!(reader.last().unwrap().timestamp(), Timestamp(5));

            reader.read_next()?;
            assert_eq!(reader.last().unwrap().timestamp(), Timestamp(10));

            reader.read_next()?;
            assert!(reader.last().is_none());

            let file = reader.close()?;
            assert!(file.is_empty());

            Ok(())
        }

        #[test]
        fn reading_same_record_from_disk_repeatedly() -> std::io::Result<()> {
            let mut in_memory = in_memory_factory();
            let file = tempfile::NamedTempFile::new().unwrap();

            let mut file = in_memory
                .drain_into_file(file.path())?
                .expect("in-memory isn't empty");

            for _ in 0..5 {
                let reader = file.read(8_192)?;
                assert_eq!(
                    reader
                        .last()
                        .expect("we never call `read_next`")
                        .timestamp(),
                    Timestamp(2)
                );
                file = reader.close()?;
            }

            let mut reader = file.read(8_192)?;
            reader.read_next()?;
            reader.read_next()?;
            reader.read_next()?;
            assert!(reader.last().is_none());

            Ok(())
        }
    }

    #[test]
    fn process_a_few_records_in_buffer() -> std::io::Result<()> {
        let dir = tempfile::tempdir()?;
        let output = dir.path().join("output");
        let mut writer = output::Writer::open(&output)?;
        let mut reader = output::Reader::open(&output)?;
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
        }))?;
        sut.push_record(Record::A(DataA {
            timestamp: Timestamp(1),
            foo: "foo".to_owned(),
        }))?;
        sut.push_record(Record::A(DataA {
            timestamp: Timestamp(3),
            foo: "foo".to_owned(),
        }))?;

        let DumpedCount(count) = sut.dump_safe(Timestamp(10))?;
        assert_eq!(count, 3);

        assert_eq!(reader.read().unwrap().timestamp(), Timestamp(1));
        assert_eq!(reader.read().unwrap().timestamp(), Timestamp(3));
        assert_eq!(reader.read().unwrap().timestamp(), Timestamp(5));
        let _ = reader.read().unwrap_err();

        Ok(())
    }

    #[test]
    fn random_million_records_is_sorted() -> std::io::Result<()> {
        const RECORDS: usize = 1_000_000;

        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("output");
        let mut writer = output::Writer::open(&output)?;
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
            sut.push_record(record)?;
        }

        let count = sut.dump_safe(Timestamp(RECORDS as u128))?;
        assert_eq!(count.0, RECORDS);

        let mut reader = output::Reader::open(&output)?;
        let mut prev_ts = reader.read()?.timestamp();
        for _ in 1..RECORDS {
            let ts = reader.read()?.timestamp();
            assert!(prev_ts <= ts);
            prev_ts = ts;
        }
        reader.read().expect_err("there must be no records left");

        Ok(())
    }
}
