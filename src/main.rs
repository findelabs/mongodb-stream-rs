use chrono::Local;
use clap::{crate_version, App, Arg};
use env_logger::{Builder, Target};
use log::LevelFilter;
use std::io::Write;
use std::error;
use db::{DB, transfer, validate};
//use bson::doc;
use std::sync::Arc;
use tokio::sync::Semaphore;

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
                .required(false)
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
                .conflicts_with("nobulk")
                .takes_value(true)
        )
        .arg(
            Arg::with_name("continue")
                .short("c")
                .long("continue")
                .required(false)
                .value_name("STREAM_CONTINUE")
                .env("STREAM_CONTINUE")
                .help("Restart streaming at the newest document")
                .takes_value(false)
        )
        .arg(
            Arg::with_name("nobulk")
                .short("n")
                .long("nobulk")
                .required(false)
                .value_name("STREAM_NOBULK")
                .env("STREAM_NOBULK")
                .help("Do not upload docs in batches")
                .conflicts_with("bulk")
                .takes_value(false)
        )
        .arg(
            Arg::with_name("threads")
                .short("t")
                .long("threads")
                .required(false)
                .value_name("STREAM_THREADS")
                .env("STREAM_THREADS")
                .help("Concurrent collections to transfer")
                .takes_value(true)
        )
        .arg(
            Arg::with_name("validate")
                .long("validate")
                .required(false)
                .value_name("MONGODB_VALIDATE")
                .env("MONGODB_VALIDATE")
                .help("Validate docs in destination")
                .takes_value(false)
        )
        .arg(
            Arg::with_name("verbose")
                .long("verbose")
                .required(false)
                .value_name("STREAM_VERBOSE")
                .env("STREAM_VERBOSE")
                .help("Enable extra verbosity")
                .takes_value(false)
        )
        .arg(
            Arg::with_name("rename")
                .long("rename")
                .required(false)
                .value_name("MONGODB_RENAME")
                .env("MONGODB_RENAME")
                .help("Rename DB at destination")
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
    let db = &opts.value_of("db").unwrap();
    let renamedb = &opts.value_of("renamedb");

    println!(
        "Starting mongodb-stream-rs:{}", 
        crate_version!(),
    );

    // Create connections to source and destination db's
    let source_db = DB::init(&source, &db, None).await?;
    let destination_db = DB::init(&destination, &db, *renamedb).await?;


    let collections = match &opts.is_present("collection") {
        true => {
            let mut vec: Vec<String> = Vec::new();
            let coll = &opts.value_of("collection").unwrap();
            vec.push(coll.to_string());
            vec
        },
        false => source_db.collections().await?
    };

    // Create vector for handles
    let mut handles = vec![];

    // Let's rate limit to just 4 collections at once
    let sem = match &opts.is_present("threads") {
        true => {
            let threads = &opts.value_of("threads").expect("unable to get threads").parse::<usize>()?;
            log::info!("Transfering {} collections at once", threads);
            Arc::new(Semaphore::new(*threads))
        },
        false => {
            log::info!("Transfering 4 collections at once");
            Arc::new(Semaphore::new(4))
        }
    };

    // Loop over collections and start uploading
    for collection in collections {

        let source = source_db.clone();
        let destination = destination_db.clone();
        let opts = opts.clone();

        // Get permission to kick off task
        let permit = Arc::clone(&sem).acquire_owned().await;

        handles.push(tokio::spawn(async move {
            let _permit = permit;
            match transfer(source.clone(), destination.clone(), opts.clone(), collection.clone()).await {
                Ok(_) => {
                    // Check docs
                    if opts.is_present("validate") {
                        match validate(source, destination, opts, collection).await {
                            Ok(_) => log::debug!("Thread shutdown"),
                            Err(e) => log::error!("Thread error: {}", e)
                        };
                    };
                    log::debug!("Thread shutdown");
                },
                Err(e) => log::error!("Thread error: {}", e)
            }
        }));

    };

    // Join all handles
    futures::future::join_all(handles).await;

    Ok(())
}
