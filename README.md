# MongoDB Stream RS

Mongodb-stream-rs is a tool that copies MongoDB databases or single collections from one replicaset or standalone to another MongoDB instance. This tool was created specifically to upload databases from a standalone MongoDB instance into an Atlas db, since mongomirror required a replicaset to function properly, and mongodump/mongorestore is too slow. 

### Installation

Once rust has been [installed](https://www.rust-lang.org/tools/install), simply run:
```
cargo install --git https://github.com/findelabs/monogdb-stream-rs.git
```

### Arguments

```
USAGE:
    mongodb-stream-rs [FLAGS] [OPTIONS] --db <MONGODB_DB> --destination_uri <STREAM_DEST> --source_uri <STREAM_SOURCE>

FLAGS:
    --db <MONGODB_DB>   # MongoDB DB Name
    --destination_uri <STREAM_DEST> # Destination standalone/replicaset
    --source_uri <STREAM_SOURCE> # Source standalone/replicaset

OPTIONS:
    --continue  # Pick up upload at the newest doc in destination
    --validate  # Confirm that all docs in destination exist in source
```
