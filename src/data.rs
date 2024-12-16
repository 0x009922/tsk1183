use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
// use std::ops::Deref;

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Serialize, Deserialize, Copy, Clone)]
pub struct Timestamp(pub u128);

#[derive(Serialize, Deserialize, Debug)]
pub struct DataA {
    pub timestamp: Timestamp,
    pub foo: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DataB {
    pub timestamp: Timestamp,
    pub bar: bool,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct DataC {
    pub timestamp: Timestamp,
    pub baz: (u32, u32),
}
#[derive(Serialize, Deserialize, Debug)]
pub struct DataD {
    pub timestamp: Timestamp,
    pub abc: (),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DataE {
    pub timestamp: Timestamp,
    pub def: Vec<u16>,
}

#[derive(Debug, Serialize, Deserialize, derive_more::From)]
pub enum Record {
    A(DataA),
    B(DataB),
    C(DataC),
    D(DataD),
    E(DataE),
}

impl Record {
    pub fn timestamp(&self) -> Timestamp {
        match &self {
            Self::A(x) => x.timestamp,
            Self::B(x) => x.timestamp,
            Self::C(x) => x.timestamp,
            Self::D(x) => x.timestamp,
            Self::E(x) => x.timestamp,
        }
    }
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp() == other.timestamp()
    }
}

impl Eq for Record {}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.timestamp().partial_cmp(&other.timestamp())
    }
}

impl Ord for Record {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp().cmp(&other.timestamp())
    }
}
