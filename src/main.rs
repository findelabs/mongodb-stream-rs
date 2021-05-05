use chrono::Local;
use clap::{crate_version, App, Arg};
use env_logger::{Builder, Target};
use log::LevelFilter;
use std::io::Write;
use std::error;
use db::DB;
use bson::doc;

mod db;

type BoxResult<T> = std::result::Result<T, Box<dyn error::Error + Send + Sync>>;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> BoxResult<()> {
    let opts = App::new("mongodb-stream-rs")
        .version(crate_version!())
        .author("Daniel F. <dan@findelabs.com>")
        .about("Stream MongoDB to MongoDB")
        .arg(
            Arg::with_name("source_uri")
                .long("source_uri")
                .required(true)
                .value_name("STREAM_SOURCE")
                .env("STREAM_SOURCE")
                .help("Source MongoDB URI")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("destination_uri")
                .long("destination_uri")
                .required(true)
                .value_name("STREAM_DEST")
                .env("STREAM_DEST")
                .help("Destination MongoDB URI")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("db")
                .short("d")
                .long("db")
                .required(true)
                .value_name("MONGODB_DB")
                .env("MONGODB_DB")
                .help("MongoDB Database")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("collection")
                .short("c")
                .long("collection")
                .required(true)
                .value_name("MONGODB_COLLECTION")
                .env("MONGODB_COLLECTION")
                .help("MongoDB Collection")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("bulk")
                .short("b")
                .long("bulk")
                .required(false)
                .value_name("STREAM_BULK")
                .env("STREAM_BULK")
                .help("Bulk stream documents")
                .takes_value(true)
        )
        .get_matches();

    // Initialize log Builder
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{{\"date\": \"{}\", \"level\": \"{}\", \"message\": \"{}\"}}",
                Local::now().format("%Y-%m-%dT%H:%M:%S:%f"),
                record.level(),
                record.args()
            )
        })
        .target(Target::Stdout)
        .filter_level(LevelFilter::Info)
        .parse_default_env()
        .init();

    // Create vars for required variables
    let source = &opts.value_of("source_uri").unwrap();
    let destination= &opts.value_of("destination_uri").unwrap();
    let collection = &opts.value_of("collection").unwrap();
    let db = &opts.value_of("db").unwrap();
    let bulk = match opts.is_present("bulk") {
        true => Some(opts.value_of("bulk").unwrap().parse::<u32>()?),
        false => None
    };

    println!(
        "Starting mongodb-stream-rs:{}", 
        crate_version!(),
    );

    // Create connections to source and destination db's
    let mut source_db = DB::init(&source, &db).await?;
    let mut destination_db = DB::init(&destination, &db).await?;

    // If bulk flag is set, use insertMany
    match bulk {
        Some(bulk_size) => {
            // Acquire cursor from source
            let (source_cursor,total) = source_db.find(collection, doc!{}, Some(bulk_size as u64)).await?;

            destination_db.bulk_insert_cursor(collection, source_cursor, total, bulk_size as usize).await?;
        }
        None => {
            // Acquire cursor from source
            let (source_cursor,total) = source_db.find(collection, doc!{}, None).await?;

            destination_db.insert_cursor(collection, source_cursor, total).await?
        }

    };

    Ok(())
}
