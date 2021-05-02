use mongodb::bson::{doc, document::Document};
//use mongodb::{options::ClientOptions, options::FindOptions, Client, Collection};
use mongodb::{options::ClientOptions, options::FindOptions, Client, Cursor};
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
        client_options.app_name = Some("json-bucket".to_string());
        Ok(Self {
            client: Client::with_options(client_options)?,
            db: db.to_owned(),
            counter: Counter::new()
        })
    }

    pub async fn find(&mut self, collection: &str, query: Document) -> BoxResult<(Cursor, f64)> {
        // Log which collection this is going into
        log::debug!("Reading {}.{}", self.db, collection);

        let find_options = FindOptions::builder()
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
        log::info!("Inserting {} docs", total);
        while let Some(doc) = cursor.next().await {
            match doc {
                Ok(doc) => {
                    match coll.insert_one(doc, None).await {
                        Ok(id) => {
                            log::info!("Inserted id: {}", id.inserted_id.to_string());
                        }
                        Err(e) => {
                            log::error!("Got error: {}", e);
                        }
                    }
                    self.counter.incr(&self.db, collection);
                }
                Err(e) => {
                    log::error!("Caught error getting next doc, skipping: {}", e);
                    continue;
                }
            };
        }
        println!("Completed {}.{}", self.db, collection);
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
            total: 0.0
        }
    }

    pub fn total(&mut self, total: f64) {
        self.total = total;
    }

    pub fn incr(&mut self, db: &str, collection: &str) {
        self.count += 1f64;
        let percent = self.count / self.total * 100.0;

        if percent - self.marker > 1.0 {
            println!("Copying {}.{}: {:.2}%", db, collection, percent);
            self.marker += 1f64;
        };
    }
}
