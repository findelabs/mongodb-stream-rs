use chrono::offset::Utc;
use mongodb::bson::{doc, document::Document};
//use mongodb::{options::ClientOptions, options::FindOptions, Client, Collection};
use mongodb::{options::ClientOptions, options::FindOptions, options::InsertManyOptions, Client, Cursor};
//use serde::{Deserialize, Serialize};
use futures::StreamExt;
use clap::ArgMatches;
use std::error;
use std::mem;
//use tokio::task;
use bson::oid::ObjectId;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone, Debug)]
pub struct DB {
    pub client: Client,
    pub db: String
}

type BoxResult<T> = std::result::Result<T, Box<dyn error::Error + Send + Sync>>;

impl DB {
    pub async fn init(url: &str, db: &str) -> BoxResult<Self> {
        let mut client_options = ClientOptions::parse(url).await?;
        client_options.app_name = Some("mongodb-stream-rs".to_string());
        Ok(Self {
            client: Client::with_options(client_options)?,
            db: db.to_owned()
        })
    }

    pub async fn collections(&self) -> BoxResult<Vec<String>> {
        Ok(self.client.database(&self.db).list_collection_names(None).await?)
    }

    pub async fn newest(&mut self, collection: &str) -> Option<String> {

        // Log which collection this is going into
        log::info!("{}.{}: Getting newest doc in destination", self.db, collection);

        // Get handle on collection
        let collection_handle = self.client.database(&self.db).collection(collection);

        let options = mongodb::options::FindOneOptions::builder().sort(doc! { "_id": -1 }).projection(doc!{"_id": 1}).build();

        // If a doc is returned, set query
        match collection_handle.find_one(Some(doc!{}), options).await {
            Ok(result) => {
                match result {
                    Some(doc) => {
                        let id = doc.get_object_id("_id").ok()?.to_string();
                        log::info!("{}.{}: Found newest doc with id: {}", self.db, collection, &id);
                        Some(id)
                    },
                    None => {
                        log::info!("{}.{}: Destination collection is empty", self.db, collection);
                        None
                    }
                }
            },
            Err(e) => {
                log::info!("{}.{}: Error finding newest doc: {}", self.db, collection, e);
                None
            }
        }
    }

    pub async fn find(&mut self, collection: &str, bulk_size: Option<u64>, newest: Option<String>) -> BoxResult<(Cursor, Counter)> {
        // Create counter
        let mut counter = Counter::new();

        // Log which collection this is going into
        log::debug!("Reading {}.{}", self.db, collection);

        let batch_size = match bulk_size {
            Some(bulk_size) => {
                if bulk_size > 6400 {
                    log::info!("{}.{}: Setting mongo cursor batch_size to 6400", self.db, collection);
                    Some(6400u32)
                } else {
                    Some(bulk_size as u32)
                }
            },
            None => None
        };

        let find_options = FindOptions::builder()
            .batch_size(batch_size)
            .sort(doc! { "_id": 1 })
            .build();

        // Get handle on collection
        let collection_handle = self.client.database(&self.db).collection(collection);

        // If --restart is set, find the oldest doc, and start there
        let query = match newest {
            Some(ref id) => doc!{ "_id": {"$gt": ObjectId::with_string(id)? } },
            None => doc!{}
        };

        // Set total in Counter
        let total = match newest {
            Some(_) => {
                log::info!("{}.{}: Calculating docs past marker", self.db, collection);
                collection_handle.count_documents(query.clone(), None).await? as f64
            },
            None => {
                log::info!("{}.{}: No newest doc found, counting all docs in collection", self.db, collection);
                self.count(collection).await?
            }
        };

        // Set counter to total
        counter.total(total);
        
        let cursor = collection_handle.find(query, find_options).await?;

        Ok((cursor, counter))
    }

    pub async fn insert_cursor(&mut self, collection: &str, mut cursor: Cursor, mut counter: Counter) -> BoxResult<()> {
        // Get handle on collection
        let coll = self.client.database(&self.db).collection(collection);

        log::info!("{}.{}: Inserting {} docs", self.db, collection, counter.total);

        // Get timestamp
        let start = Utc::now().timestamp();
        
        while let Some(doc) = cursor.next().await {
            match doc {
                Ok(doc) => {
                    match coll.insert_one(doc, None).await {
                        Ok(id) => {
                            log::debug!("{}.{}: Inserted id: {}", self.db, collection, id.inserted_id.to_string());
                        }
                        Err(e) => {
                            log::debug!("{}.{}: Got error: {}", self.db, collection, e);
                        }
                    }
                    counter.incr(&self.db, collection, 1.0, start);
                }
                Err(e) => {
                    log::error!("{}.{}: Caught error getting next doc: {}", self.db, collection, e);
                    continue;
                }
            };
        }
        log::info!("{}.{}: Completed", self.db, collection);
        Ok(())
    }

    pub async fn bulk_insert_cursor(&mut self, collection: &str, mut cursor: Cursor, mut counter: Counter, bulk_count: usize) -> BoxResult<()> {
        // Get handle on collection
        let coll = self.client.database(&self.db).collection(collection);

        if counter.total != 0.0 {
            log::info!("{}.{}: Bulk inserting {} docs in batches of {}", self.db, collection, counter.total, bulk_count);
        } else {
            log::info!("{}.{}: There are {} docs to upload", self.db, collection, counter.total);
        };

        let insert_many_options = InsertManyOptions::builder()
            .ordered(Some(false))
            .build();

        // Create vector of documents to bulk upload
        let mut bulk: Vec<Document> = Vec::with_capacity(bulk_count);

        // Get timestamp
        let start = Utc::now().timestamp();
        
        // Set count
        let mut count: usize = 0;

        // Create vector for task handles
        let mut handles = vec![];

        // Let's rate limit to just 10 uploads at once
        let sem = Arc::new(Semaphore::new(4));

        // Good article about memory swapping: https://stackoverflow.com/questions/50970102/is-there-a-way-to-fill-up-a-vector-for-bulk-inserts-with-the-mongodb-driver-and
        while let Some(doc) = cursor.next().await {
            match doc {
                Ok(d) => {
                    // Push to vec, incr counter, and print debug log
                    bulk.push(d);
                    count += 1;
                    log::debug!("{}.{}: inserted doc: {}/{}", self.db, collection, count, bulk_count);

                    // If counter is greater or equal to bulk_count
                    if count >= bulk_count || (count + counter.count as usize) == counter.total as usize {

                        // Create a new empty vec, then swap, to avoid clone()
                        let mut tmp_bulk: Vec<Document> = Vec::with_capacity(bulk_count);
                        mem::swap(&mut bulk, &mut tmp_bulk);
                    
                        // Get clones for the threads
                        let coll_clone = coll.clone();
                        let options = insert_many_options.clone();

                        // Get permission to kick off task
                        let permit = Arc::clone(&sem).acquire_owned().await;

                        handles.push(tokio::spawn(async move {
                            let _permit = permit;
                            match coll_clone.insert_many(tmp_bulk, options.clone()).await {
                                Ok(_) => {
                                    log::debug!("Bulk inserted {} docs", bulk_count);
                                }
                                Err(e) => {
                                    log::debug!("Got error with insertMany: {}", e);
                                }
                            }
                        }));
                        count = 0;
                        counter.incr(&self.db, collection, bulk_count as f64, start);
                    } else {
                        continue
                    }
                }
                Err(e) => {
                    log::error!("{}.{} Caught error getting next doc: {}", self.db, collection, e);
                    continue;
                }
            }
        }

        // Wait for all handles to complete
        futures::future::join_all(handles).await;

        log::info!("{}.{}: Completed", self.db, collection);
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn count(&self, collection: &str) -> BoxResult<f64> {
        // Log that we are trying to list collections
        log::debug!("Getting document count in {}", self.db);

        let collection = self.client.database(&self.db).collection(collection);

        match collection.estimated_document_count(None).await {
            Ok(count) => {
                log::debug!("Successfully counted docs in {}", self.db);
                Ok(count as f64)
            }
            Err(e) => {
                log::error!("Got error {}", e);
                Err(Box::new(e))
            }
        }
    }

    #[allow(dead_code)]
    pub async fn get_indexes(&self, collection: &str) -> BoxResult<Document> {
        // Log that we are trying to list collections
        log::debug!("Getting indexes in {}", self.db);

        let database = self.client.database(&self.db);
        let command = doc! { "listIndexes": collection };

        match database.run_command(command, None).await {
            Ok(indexes) => {
                log::debug!("Successfully got indexes in {}.{}", self.db, collection);
                let index_cursor = indexes.get_document("cursor").expect("Successfully got indexes, but failed to extract cursor").clone();
                Ok(index_cursor)
            }
            Err(e) => {
                log::error!("Got error {}", e);
                Err(Box::new(e))
            }
        }
    }
}

#[derive(Clone, Copy,  Debug)]
pub struct Counter {
    pub count: f64,
    pub marker: f64,
    pub total: f64
}

impl Counter {
    pub fn new() -> Counter {
        Counter {
            count: 0.0,     // Count of docs uploaded
            marker: 0.0,    // Percentage tracker
            total: 0.0,     // Total count of all docs in collection
        }
    }

//    pub fn set(&mut self, count: i64) {
//        self.count = count as f64;
//    }

    pub fn total(&mut self, total: f64) {
        self.total = total;
    }

    pub fn incr(&mut self, db: &str, collection: &str, count: f64, start: i64) {
        self.count += count;
        let percent = self.count / self.total * 100.0;

        // Get time elapsed
        let now = Utc::now().timestamp();
        let delta = now - start;

        // Get insert rate
        let rate = self.count / delta as f64;

        if self.count >= self.total {
            log::info!("{}.{}: 100%, {:.2}/s, {}/{}", db, collection, rate, self.total, self.total);
        } else if percent - self.marker > 1.0 {
            log::info!("{}.{}: {:.2}%, {:.2}/s, {}/{}", db, collection, percent, rate, self.count, self.total);
            self.marker += 1f64;
        };
    }
}

pub async fn transfer(mut source_db: DB, mut destination_db: DB, opts: ArgMatches<'_>, collection: String) -> BoxResult<()> {

    let bulk_size = match opts.is_present("bulk") {
        true => opts.value_of("bulk").unwrap().parse::<u32>()?,
        false => 2000u32
    };

    // If --restart is set, find newest doc
    let newest_doc = match opts.is_present("restart") {
        true => destination_db.newest(&collection).await,
        false => None
    };
    
    // If bulk flag is set, use insertMany
    match opts.is_present("nobulk") {
        false => {
            // Acquire cursor from source
            let (source_cursor,counter) = source_db.find(&collection, Some(bulk_size as u64), newest_doc).await?;
            destination_db.bulk_insert_cursor(&collection, source_cursor, counter, bulk_size as usize).await?;
        }
        true => {
            // Acquire cursor from source
            let (source_cursor,counter) = source_db.find(&collection, None, newest_doc).await?;
            destination_db.insert_cursor(&collection, source_cursor, counter).await?
        }   
    };

    Ok(())
}
