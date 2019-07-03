# AllyDB


AllyDB is a simple key value storage, which stores data in files.

Supported operations:

```
void put(byte[] key, byte[] value)
void put(byte[] existingKey, byte[] newValue) //replace value under existing key
byte[] get(byte[] key)      
```

There is a gRPC interface provided (check Abby.proto file)

The gRPC interface supports Reflection API

Only hashes of the keys are stored.

Values are compressed on put operation and decompressed on get operation,

Index, buffers, cache:

* Write buffer
* Edit buffer
* Read cache
* In-memory index

Internal scheduled operations:
* Writing the content of the write buffer to disk
* Writing the content of the edit buffer to disk
* Write the index to disk
* Garbage collection (Cleaning dirty files after edit operation)

Index file structure:

`sha256(key) | file name | line number`

Storage file(s) structure:

`sha256(key) | gzip(value)`

Storage file(s) naming:
```
(prefix 's') + (uuid with all '-' replaced with '_') + .ally

Example: s3cb2a8b0_fc32_4ec3_a1e7_945cb0cfa073.ally
```

Just do 
`mvn clean compile`
before running

Plans for the future:

* [ ] Tests
* [ ] Javadoc
* [ ] More comments
* [ ] Class for index file
* [ ] Another garbage collector, which doesnt dirty files collection, 
but scans all storage files 
and performs a garbage collection (should be running less frequently)
* [ ] Separating low and high level operations
* [ ] Considering RandomAccessFile, FileChannels, etc.
* [ ] Concurrent operations
* [ ] Better logging
* [ ] Hash collision handling (This should require saving original key)
