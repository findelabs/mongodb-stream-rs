# MongoDB Stream RS

Mongodb-stream-rs is a tool that copies MongoDB databases or single collections from one replicaset or standalone to another MongoDB instance. This tool was created specifically to upload databases from a standalone MongoDB instance into an Atlas db, since mongomirror required a replicaset to function properly, and mongodump/mongorestore is too slow. 

### Installation

Once rust has been [installed](https://www.rust-lang.org/tools/install), simply run:
```
cargo install --git https://github.com/findelabs/monogdb-stream-rs.git
```

### Technical

This tool is written in rust and leverages the tokio runtime in order to send multiple collection to the destination database at once. By default mongodb-stream-rs will upload four collections in parellel. By default, uploads are transmitted in batches of 2000 docs, but this option can be changed with the `--bulk` flag. You can override this default with the `--nobulk` flag in order to have this tool upload one doc at a time.

If only a database name is passed to the app, then this tool will upload all collections within the db. However, you can specify a single collection to upload with `--collection`.

### Arguments

```
USAGE:
    mongodb-stream-rs [FLAGS] [OPTIONS] --db <MONGODB_DB> --destination_uri <STREAM_DEST> --source_uri <STREAM_SOURCE>

FLAGS:
    -c, --continue    Restart streaming at the newest document
    -h, --help        Prints help information
    -n, --nobulk      Do not upload docs in batches
        --validate    Validate docs in destination
    -V, --version     Prints version information

OPTIONS:
    -b, --bulk <STREAM_BULK>                 Bulk stream documents [env: STREAM_BULK=]
    -c, --collection <MONGODB_COLLECTION>    MongoDB Collection [env: MONGODB_COLLECTION=]
    -d, --db <MONGODB_DB>                    MongoDB Database [env: MONGODB_DB=]
        --destination_uri <STREAM_DEST>      Destination MongoDB URI [env: STREAM_DEST=]
        --source_uri <STREAM_SOURCE>         Source MongoDB URI [env: STREAM_SOURCE=]
    -t, --threads <STREAM_THREADS>           Concurrent collections to transfer [env: STREAM_THREADS=]
```

### Future

We plan to utilize watch() on a db, nce the mongodb rust driver supports opening a watch on a database. This will effectively remove the need for `--continue`, especially if updates are being done on the source db.
