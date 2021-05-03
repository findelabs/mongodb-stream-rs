use std::sync::Arc;
use chrono::offset::Utc;
use std::sync::Mutex;
use mongodb::bson::{doc, document::Document};
//use mongodb::{options::ClientOptions, options::FindOptions, Client, Collection};
use mongodb::{options::ClientOptions, options::FindOptions, options::InsertManyOptions, Client, Cursor};
//use serde::{Deserialize, Serialize};
use futures::StreamExt;
//use clap::ArgMatches;
use std::error;


#[derive(Clone, Debug)]
pub struct DB {
    pub client: Client,
    pub db: String,
    pub counter: Counter
}

type BoxResult<T> = std::result::Result<T, Box<dyn error::Error + Send + Sync>>;

impl DB {
    pub async fn init(url: &str, db: &str) -> BoxResult<Self> {
        let mut client_options = ClientOptions::parse(url).await?;
        client_options.app_name = Some("mongodb-stream-rs".to_string());
        Ok(Self {
            client: Client::with_options(client_options)?,
            db: db.to_owned(),
            counter: Counter::new()
        })
    }

    pub async fn find(&mut self, collection: &str, query: Document, bulk_size: Option<u64>) -> BoxResult<(Cursor, f64)> {
        // Log which collection this is going into
        log::debug!("Reading {}.{}", self.db, collection);

        let batch_size = match bulk_size {
            Some(bulk_size) => {
                if bulk_size > 64000 {
                    log::info!("Setting mongo cursor batch_size to 64000");
                    Some(64000u32)
                } else {
                    Some(bulk_size as u32)
                }
            },
            None => None
        };

        let find_options = FindOptions::builder()
            .batch_size(batch_size)
            .sort(doc! { "_id": -1 })
            .build();

        // Set total in Counter
        let total = self.count(collection).await?;
        self.counter.total(total);
        
        let collection = self.client.database(&self.db).collection(collection);
        let cursor = collection.find(query, find_options).await?;

        Ok((cursor, self.counter.total))
    }

    pub async fn insert_cursor(&mut self, collection: &str, mut cursor: Cursor, total: f64) -> BoxResult<()> {
        let coll = self.client.database(&self.db).collection(collection);
        self.counter.total(total);
        log::info!("Inserting {} docs to {}.{}", total, self.db, collection);

        // Get timestamp
        let start = Utc::now().timestamp();
        
        while let Some(doc) = cursor.next().await {
            match doc {
                Ok(doc) => {
                    match coll.insert_one(doc, None).await {
                        Ok(id) => {
                            log::debug!("Inserted id: {}", id.inserted_id.to_string());
                        }
                        Err(e) => {
                            log::debug!("Got error: {}", e);
                        }
                    }
                    self.counter.incr(&self.db, collection, 1.0, start);
                }
                Err(e) => {
                    log::error!("Caught error getting next doc, skipping: {}", e);
                    continue;
                }
            };
        }
        log::info!("Completed {}.{}", self.db, collection);
        Ok(())
    }

    pub async fn bulk_insert_cursor(&mut self, collection: &str, mut cursor: Cursor, total: f64, bulk_count: usize) -> BoxResult<()> {
        let coll = self.client.database(&self.db).collection(collection);
        self.counter.total(total);
        log::info!("Bulk inserting {} docs to {}.{} in batches of {}", total, self.db, collection, bulk_count);

        let insert_many_options = InsertManyOptions::builder()
            .ordered(Some(false))
            .build();

        // Create vector of documents to bulk upload
//        let mut bulk = Bulk::new(bulk_count);
        let mut bulk = Vec::with_capacity(bulk_count);
        // Get timestamp
        let start = Utc::now().timestamp();
        
        // Set counter
        let mut counter: usize = 0;

        while let Some(doc) = cursor.next().await {
            match doc {
                Ok(d) => {
                    // Push to vec, incr counter, and print debug log
                    bulk.push(d);
                    counter += 1;
                    log::debug!("inserted doc: {}/{}", counter, bulk_count);

                    // If counter is greater or equal to bulk_count
                    if counter >= bulk_count {
                        match coll.insert_many(bulk.clone(), insert_many_options.clone()).await {
                            Ok(_) => {
                                log::debug!("Bulk inserted {} docs", bulk_count);
                            }
                            Err(e) => {
                                log::debug!("Got error with insertMany: {}", e);
                            }
                        }
                        counter = 0;
                        bulk.clear();
                        self.counter.incr(&self.db, collection, bulk_count as f64, start);
                    } else {
                        continue
                    }
//                    match bulk.push(d) {
//                        Some(values) => {
//                            log::debug!("Bulk inserting {} docs", bulk_count);
//                            match coll.insert_many(values, insert_many_options.clone()).await {
//                                Ok(_) => {
//                                    log::debug!("Bulk inserted {} docs", bulk_count);
//                                }
//                                Err(e) => {
//                                    log::debug!("Got error with insertMany: {}", e);
//                                }
//                            }
//                            self.counter.incr(&self.db, collection, bulk_count as f64, start);
//                        }
//                        None => {
//                            log::debug!("inserted doc: {}/{}", bulk.len(), bulk_count);
//                            continue
//                        }
//                    }
                }
                Err(e) => {
                    log::error!("Caught error getting next doc, skipping: {}", e);
                    continue;
                }
            }
        }
        log::info!("Completed {}.{}", self.db, collection);
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
            count: 0.0,
            marker: 0.0,
            total: 0.0,
        }
    }

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

        if percent - self.marker > 1.0 {
            log::info!("{}.{}: {:.2}%, {:.2}/s, {}/{}", db, collection, percent, rate, self.count, self.total);
            self.marker += 1f64;
        };
    }
}

#[derive(Debug, Clone)]
pub struct Bulk {
    pub inner: Arc<Mutex<Vec<Document>>>
}

impl Bulk {
    pub fn new(size: usize) -> Bulk {
        Bulk { 
            inner: Arc::new(Mutex::new(Vec::with_capacity(size))) 
        }
    }

    pub fn push(&mut self, doc: Document) -> Option<Vec<Document>> {
        let mut me = self.inner.lock().expect("failed locking mutex");
        me.push(doc);

        if me.len() >= me.capacity() {
            let values = me.to_vec();
            me.clear();
            return Some(values)
        } else {
            return None
        }
    }

    pub fn len(&self) -> usize {
        let me = self.inner.lock().expect("failed locking mutex");
        me.len()
    }
}
