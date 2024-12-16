use crate::data::Record;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

/// Write records into the output file.
#[derive(Debug)]
pub struct Writer {
    buf_writer: BufWriter<File>,
}

impl Writer {
    /// Open the writer.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        Ok(Self {
            buf_writer: BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(path)?,
            ),
        })
    }

    /// Write a record into the file, without caring about ordering.
    pub fn write(&mut self, record: &Record) -> std::io::Result<()> {
        if let Err(err) = bincode::serialize_into(&mut self.buf_writer, record) {
            match *err {
                bincode::ErrorKind::Io(err) => return Err(err),
                other => panic!("intentionally not covering serialisation errors: {other}"),
            }
        }
        Ok(())
    }

    /// Flush buffered data.
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.buf_writer.flush()
    }
}

/// Read records from the output file.
pub struct Reader {
    buf_reader: BufReader<File>,
}

impl Reader {
    /// Open the reader.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        Ok(Self {
            buf_reader: BufReader::new(OpenOptions::new().read(true).open(path)?),
        })
    }

    /// Read a record, assuming that it **must** be available already.
    pub fn read(&mut self) -> std::io::Result<Record> {
        match bincode::deserialize_from(&mut self.buf_reader) {
            Ok(x) => Ok(x),
            Err(err) => match *err {
                bincode::ErrorKind::Io(err) => Err(err),
                other => panic!("intentionally not covering deserialisation errors: {other}"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::*;
    use assert_matches::assert_matches;

    #[test]
    fn write_and_read_few_records() -> std::io::Result<()> {
        let file = tempfile::NamedTempFile::new()?;

        let mut writer = Writer::open(file.path())?;
        let mut reader = Reader::open(file.path())?;

        writer.write(&Record::D(DataD {
            timestamp: Timestamp(51),
            abc: (),
        }))?;

        writer.write(&Record::C(DataC {
            timestamp: Timestamp(1),
            baz: (0, 1),
        }))?;
        writer.write(&Record::D(DataD {
            timestamp: Timestamp(100),
            abc: (),
        }))?;
        writer.flush()?;

        assert_matches!(reader.read()?, Record::D(x) if x.timestamp == Timestamp(51));
        assert_matches!(reader.read()?, Record::C(x) if x.timestamp == Timestamp(1));
        assert_matches!(reader.read()?, Record::D(x) if x.timestamp == Timestamp(100));

        Ok(())
    }
}
