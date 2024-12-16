# tsk1183

To produce a globally ordered output from multiple channels, we need to store all data packages somewhere as
well as the latest timestamps from all channels. Once our channels are all _surely_ ahead of what we've buffered, we
could
put those records \[that are earlier than the minimal latest timestamp among the channels\] into the output file.

Since we cannot store all records in memory, I store only a fixed limited amount of them on a binary heap. When the heap
is full, I drain it into a file (different one each time). When the time comes to produce the output, I perform
merge-sort, reading all files simultaneously with small chunks. After each such "dump", I notify the reader about the
new amount of records available for reading.

As a **serialisation** format I use `bincode` as it makes easy to serialise records of data serially in files.

I decided not to use **async**, but rely on `std` and threads, to keep the solution simpler.

I wrote **tests** to cover the most complex core functionality. However, I found it not very trivial to write tests to
reproduce actual edge cases mentioned in the case:

- Some channels are empty
- Some channels are very rarefied

But anyway:

```shell
cargo test
```

Further **optimisation**:

- Currently, the whole process of writing/reading files could be triggered by a single incoming record (if the
  conditions are matching). This could be not very efficient. Instead, **the dumping could be throttled**, and triggered
  once per $N$ milliseconds (given the condition match). This isn't trivial to implement using only `std`, but would be
  so with e.g. `tokio`.
- Currently, the in-memory buffer is dumped into a file before the beginning of each merge sort. This isn't optimal.
  Instead, **the in-memory buffer could be used in-place.** For example, it could be handled specially in the merge-sort
  loop, or I could shift to using some `dyn MergeSortReader` approach.

It isn't very representable, but you can run an **example**:

```bash
cargo run --example indefinite
```
