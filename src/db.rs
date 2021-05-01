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
}

type BoxResult<T> = std::result::Result<T, Box<dyn error::Error + Send + Sync>>;

impl DB {
    pub async fn init(url: &str, db: &str) -> BoxResult<Self> {
        let mut client_options = ClientOptions::parse(url).await?;
        client_options.app_name = Some("json-bucket".to_string());
        Ok(Self {
            client: Client::with_options(client_options)?,
            db: db.to_owned(),
        })
    }

    pub async fn find(&self, collection: &str, query: Document) -> BoxResult<(Cursor, i64)> {
        // Log which collection this is going into
        log::debug!("Reading {}.{}", self.db, collection);

        let find_options = FindOptions::builder()
            .sort(doc! { "_id": -1 })
            .build();

        let count = self.count(collection).await?;
        let collection = self.client.database(&self.db).collection(collection);
        let cursor = collection.find(query, find_options).await?;

        Ok((cursor, count))
    }

    pub async fn insert_cursor(&self, collection: &str, mut cursor: Cursor, count: i64) -> BoxResult<()> {
        let collection = self.client.database(&self.db).collection(collection);
        let counter: i64 = 0;
        log::info!("Inserting {} docs", &count);
        while let Some(doc) = cursor.next().await {
            match doc {
                Ok(doc) => {
                    match collection.insert_one(doc, None).await {
                        Ok(id) => {
                            log::info!("Inserted id: {}", id.inserted_id.to_string());
                            increment(counter, 1);
                        }
                        Err(e) => {
                            log::error!("Got error: {}", e);
                        }
                    }
                }
                Err(e) => {
                    log::error!("Caught error getting next doc, skipping: {}", e);
                    continue;
                }
            };
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn count(&self, collection: &str) -> BoxResult<i64> {
        // Log that we are trying to list collections
        log::debug!("Getting document count in {}", self.db);

        let collection = self.client.database(&self.db).collection(collection);

        match collection.estimated_document_count(None).await {
            Ok(count) => {
                log::debug!("Successfully counted docs in {}", self.db);
                Ok(count)
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

fn increment(number: i64, add: i64) -> i64 {                      // Function adds a number to a number
    number + add                                            // Return number plus number
}

