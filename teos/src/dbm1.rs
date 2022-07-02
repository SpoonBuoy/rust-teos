use async_trait::async_trait;
use sqlx::Error;
use anyhow::*;
use sqlx::AnyConnection;
use sqlx::Connection;
use sqlx::Executor;
use std::str::FromStr;
use sqlx::Transaction;
use sqlx::any::AnyArguments;
use sqlx::any::AnyQueryResult;
use sqlx::any::AnyRow;
use sqlx::error;
use sqlx::query::Query;
use std::result::Result::Ok;
use std::collections::{HashMap, HashSet};
//for try_next
use futures::StreamExt;
use futures::TryStreamExt;
use sqlx::Row;

use bitcoin::consensus;
use bitcoin::hashes::Hash;
use bitcoin::secp256k1::SecretKey;
use bitcoin::BlockHash;
use serde::Serialize;

use teos_common::appointment::{compute_appointment_slots, Appointment, Locator};
use teos_common::constants::ENCRYPTED_BLOB_MAX_SIZE;
//use teos_common::dbm::{DatabaseConnection, DatabaseManager, Error};
use teos_common::UserId;

use crate::extended_appointment::{ExtendedAppointment, UUID};
use crate::gatekeeper::UserInfo;
use crate::responder::{ConfirmationStatus, TransactionTracker};
#[derive(Debug)]
pub enum Errorr {
    AlreadyExists,
    MissingForeignKey,
    MissingField,
    NotFound,
}
const TABLES: [&str; 1] = [
    "CREATE TABLE IF NOT EXISTS temp (
    user_id INT PRIMARY KEY,
    available_slots INT NOT NULL,
    subscription_expiry INT NOT NULL
)"];
//Traits
pub trait DatabaseConnection{
    fn get_connection(&self) -> &AnyConnection;

    fn get_mut_connection(&mut self) -> &mut AnyConnection;
}
#[async_trait]
pub trait DatabaseManager: Sized {
    async fn create_tables(&mut self, tables: Vec<&str>) -> Result<()>;
    async fn store_data(&mut self, query: &str) -> Result<(), Error>;
    async fn remove_data(&mut self, query: &str) -> Result<(), Error>;
    async fn update_data(&mut self, query: &str) -> Result<(), Error>;
 }
//Traits end

#[derive(Debug)]
pub struct DBM{
    connection: AnyConnection,
    db_type : i8
}

//Implement traits for DBM

impl DatabaseConnection for DBM{
    fn get_connection(&self) -> &AnyConnection{
        return &self.connection
    }

    fn get_mut_connection(&mut self) -> &mut AnyConnection{
        return &mut self.connection
    }
}
#[async_trait]
impl DatabaseManager for DBM{
   async fn create_tables(&mut self, tables: Vec<&str>) -> Result<()>{
       let mut tx = self.get_mut_connection().begin().await.unwrap();
        for table in tables.iter(){
           tx.execute(
               sqlx::query(table)
           ).await?;
       }
       tx.commit().await?;

       Ok(())
   }
   async fn store_data(&mut self, q: &str) -> Result<(), Error>{
       let mut tx = self.connection.begin().await.unwrap();
       //tx.commit().await?;
       let res = sqlx::query(q).
                   execute(&mut tx)
                   .await;
        match res{
            Ok(_) =>{
                tx.commit().await?;   
                Ok(())
            }
            Err(e) => Err(e)
        }
   }
   async fn remove_data(&mut self, q: &str) -> Result<(), Error>{
        let mut tx = self.connection.begin().await.unwrap();
        let res = sqlx::query(q).
                   execute(&mut tx)
                   .await.unwrap().rows_affected();
        //Error checking block to be implemented
        match res{
            0 => Err(()),
            _ =>{
                tx.commit().await?; 
                Ok(())
            }
        } 
   }
   async fn update_data(&mut self, q: &str) -> Result<(), Error>{
        self.remove_data(q).await
   }

}

//Implement traits end

impl DBM{
    //Creates a new DBM instance
    pub async fn new(connection : AnyConnection, db_type: i8) -> Result<Self>{
        let mut dbm = Self{
            connection,
            db_type
        };
        let p = dbm.create_tables(Vec::from_iter(TABLES)).await?;
        println!("Created Table {:?}", p);
        Ok(dbm)
    }

    //Stores a user 
    pub(crate) async fn store_user(&mut self, user_id: UserId, user_info: &UserInfo) -> Result<(), Error> {
        let query =
            format!(
                "INSERT INTO temp (user_id, available_slots, subscription_expiry) VALUES ({}, {}, {})",
                user_id.serialize(),
                user_info.available_slots,
                user_info.subscription_expiry,
            );

        match self.store_data(&query).await {
            Ok(x) => {
                println!("User successfully stored: {}", user_id);
                Ok(x)
            }
            Err(e) => {
                println!("Couldn't store user: {}. Error: {:?}", user_id, e);
                Err(e)
            }
        }
    }

    /// Updates an existing user ([UserInfo]) in the database.
    pub(crate) async fn update_user(&mut self, user_id: UserId, user_info: &UserInfo) {
        let query =
        format!(
            "UPDATE users SET available_slots=({}), subscription_expiry=({}) WHERE user_id=({})",
            user_info.available_slots,
            user_info.subscription_expiry,
            user_id.to_vec(),
        );
        match self.update_data(&query).await {
            Ok(_) => {
                log::debug!("User's info successfully updated: {}", user_id);
            }
            Err(_) => {
                log::error!("User not found, data cannot be updated: {}", user_id);
            }
        }
    }

      /// Loads the associated appointments ([Appointment]) of a given user ([UserInfo]).
      pub(crate) async fn load_user_appointments(&self, user_id: UserId) -> HashMap<UUID, u32> {
        let mut con = self.connection;
        let q = format!(
            "SELECT UUID, encrypted_blob FROM appointments WHERE user_id={}",
            user_id
        );
        let mut rows = 
                sqlx::query(&q)
                .fetch(&mut con);

        let mut appointments = HashMap::new();
        while let Some(inner_row) = rows.try_next().await.unwrap() {
            let raw_uuid: Vec<u8> = inner_row.get(0).unwrap();
            let uuid = UUID::from_slice(&raw_uuid[0..20]).unwrap();
            let e_blob: Vec<u8> = inner_row.get(1).unwrap();

            appointments.insert(
                uuid,
                compute_appointment_slots(e_blob.len(), ENCRYPTED_BLOB_MAX_SIZE),
            );
        }

        appointments
    }

       /// Loads all users from the database.
       pub(crate) async fn load_all_users(&mut self) -> HashMap<UserId, UserInfo> {
        let mut users = HashMap::new();
        let mut stmt = self.connection.begin().await.unwrap();
        let mut rows =  
            sqlx::query(
                "SELECT * FROM users"
            ).fetch(&mut stmt);
        while let Some(row) = rows.try_next().await.unwrap(){
            let raw_userid: Vec<u8> = row.get(0).unwrap();
            let user_id = UserId::from_slice(&raw_userid).unwrap();
            let slots = row.get(1).unwrap();
            let expiry = row.get(2).unwrap();

            users.insert(
                user_id,
                UserInfo::with_appointments(slots, expiry, self.load_user_appointments(user_id)),
            );
        }
        users
    }

    /// Removes some users from the database in batch.
    /// Confusion in iter and limits
    pub(crate) async fn batch_remove_users(&mut self, users: &HashSet<Vec<i32>>) -> usize {
        let limit = 2;
        //let limit = self.connection.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER) as usize;
        let mut tx = self.connection.begin().await.unwrap();
        let iter = users
            .iter();
            // .map(|uuid| uuid.serialize())
            // .collect::<Vec<Vec<u8>>>();

        for chunk in iter {
            let mut placeholders = "$1".to_string();
            let mut i = 0;
            while i<chunk.len() - 1{
                placeholders += &format!(", ${}", i+2);
                i = i + 1;
            }
            let query_str = format!("DELETE FROM temp WHERE user_id IN ({})", placeholders);
            println!("query: {:?}", query_str);
            let mut query = sqlx::query(&query_str) as Query<sqlx::Any, sqlx::any::AnyArguments>;
            for i in chunk{
                query = query.bind(i);
            }
            match query.execute(&mut tx).await{
                Ok(_) => log::debug!("Users deletion added to db transaction"),
                Err(e) => log::error!("Couldn't add deletion query to transaction. Error: {:?}", e),
            }
        }
        match tx.commit().await {
            Ok(_) => println!("Users successfully deleted"),
            Err(e) => println!("Couldn't delete users. Error: {:?}", e),
        }

        (users.len() as f64 / limit as f64).ceil() as usize
    }

    //Stores an appointment into the database
    pub(crate) async fn store_appointment(
        &self,
        uuid: UUID,
        appointment: &ExtendedAppointment
    ) -> Result<(), Error> {
        let query =
        format!(
            "INSERT INTO appointments (UUID, locator, encrypted_blob, to_self_delay, user_signature, start_block, user_id) VALUES ({}, {}, {}, {}, {}, {}, {})",
            uuid.to_vec(),
            appointment.locator().to_vec(),
            appointment.encrypted_blob(),
            appointment.to_self_delay(),
            appointment.user_signature,
            appointment.start_block,
            appointment.user_id.to_vec(),
        );
        match self.store_data(&query).await {
            Ok(x) => {
                log::debug!("Appointment successfully stored: {}", uuid);
                Ok(x)
            }
            Err(e) => {
                log::error!("Couldn't store appointment: {}. Error: {:?}", uuid, e);
                Err(e)
            }
        }
    }

     /// Updates an existing [Appointment] in the database.
     pub(crate) async fn update_appointment(&self, uuid: UUID, appointment: &ExtendedAppointment) {
        // DISCUSS: Check what fields we'd like to make updatable. e_blob and signature are the obvious, to_self_delay and start_block may not be necessary (or even risky)
        let query =
        format!(
            "UPDATE appointments SET encrypted_blob=({}), to_self_delay=({}), user_signature=({}), start_block=({}) WHERE UUID=({})",
            appointment.encrypted_blob(),
            appointment.to_self_delay(),
            appointment.user_signature,
            appointment.start_block,
            uuid.to_vec(),
        );
        match self.update_data(&query).await {
            Ok(_) => {
                log::debug!("Appointment successfully updated: {}", uuid);
            }
            Err(e) => {
                log::error!("Appointment not found, data cannot be updated: {}", uuid);
            }
        }
    }

     /// Loads an [Appointment] from the database.
     pub(crate) async fn load_appointment(&self, uuid: UUID) -> Option<ExtendedAppointment> {
        let key = uuid.to_vec();
        let mut tx = self.connection.begin().await.unwrap();
        let q = format!(
            "SELECT * FROM appointments WHERE UUID=({})",
            key
        );
       let row = sqlx::query(&q)
       .fetch_one(&mut tx)
       .await;
       match row{
            Ok(row) => {
                let raw_locator: Vec<u8> = row.get(1).unwrap();
                let locator = Locator::from_slice(&raw_locator).unwrap();
                let raw_userid: Vec<u8> = row.get(6).unwrap();
                let user_id = UserId::from_slice(&raw_userid).unwrap();
                let appointment = Appointment::new(locator, row.get(2).unwrap(), row.get(3).unwrap());
                Some(ExtendedAppointment::new(
                    appointment,
                    user_id,
                    row.get(4).unwrap(),
                    row.get(5).unwrap(),
                ))
            }
            Err(_) => {
                None
            }
        }
    }

       /// Loads all appointments from the database.
    pub(crate) async fn load_all_appointments(&self) -> HashMap<UUID, ExtendedAppointment> {
        let mut appointments = HashMap::new();
        let mut tx = self.connection.begin().await.unwrap();
        let mut rows = sqlx::query("SELECT * FROM appointments as a LEFT JOIN trackers as t ON a.UUID=t.UUID WHERE t.UUID IS NULL")
        .fetch(&mut tx);
        while let Some(row) = rows.try_next().await.unwrap() {
            let raw_uuid: Vec<u8> = row.get(0).unwrap();
            let uuid = UUID::from_slice(&raw_uuid[0..20]).unwrap();
            let raw_locator: Vec<u8> = row.get(1).unwrap();
            let locator = Locator::from_slice(&raw_locator).unwrap();
            let raw_userid: Vec<u8> = row.get(6).unwrap();
            let user_id = UserId::from_slice(&raw_userid).unwrap();

            let appointment = Appointment::new(locator, row.get(2).unwrap(), row.get(3).unwrap());

            appointments.insert(
                uuid,
                ExtendedAppointment::new(
                    appointment,
                    user_id,
                    row.get(4).unwrap(),
                    row.get(5).unwrap(),
                ),
            );
        }
        appointments
    }

      /// Removes an [Appointment] from the database.
      pub(crate) async fn remove_appointment(&self, uuid: UUID) {
        let query = format!(
            "DELETE FROM appointments WHERE UUID={}",
            uuid.to_vec()
        );
        match self.remove_data(&query).await {
            Ok(_) => {
                log::debug!("Appointment successfully removed: {}", uuid);
            }
            Err(_) => {
                log::error!("Appointment not found, data cannot be removed: {}", uuid);
            }
        }
    }

    //  /// Removes some appointments from the database in batch and updates the associated users giving back
    // /// the freed appointment slots
    // pub(crate) fn batch_remove_appointments(
    //     &mut self,
    //     appointments: &HashSet<UUID>,
    //     updated_users: &HashMap<UserId, UserInfo>,
    // ) -> usize {
    //     let limit = self.connection.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER) as usize;
    //     let tx = self.connection.transaction().unwrap();
    //     let iter = appointments
    //         .iter()
    //         .map(|uuid| uuid.to_vec())
    //         .collect::<Vec<Vec<u8>>>();

    //     for chunk in iter.chunks(limit) {
    //         let query = "DELETE FROM appointments WHERE UUID IN ".to_owned();
    //         let placeholders = format!("(?{})", (", ?").repeat(chunk.len() - 1));

    //         match tx.execute(
    //             &format!("{}{}", query, placeholders),
    //             params_from_iter(chunk),
    //         ) {
    //             Ok(_) => log::debug!("Appointments deletion added to db transaction"),
    //             Err(e) => log::error!("Couldn't add deletion query to transaction. Error: {:?}", e),
    //         }
    //     }

    //     for (id, info) in updated_users.iter() {
    //         let query = "UPDATE users SET available_slots=(?1) WHERE user_id=(?2)";
    //         match tx.execute(query, params![info.available_slots, id.to_vec(),]) {
    //             Ok(_) => log::debug!("User update added to db transaction"),
    //             Err(e) => log::error!("Couldn't add update query to transaction. Error: {:?}", e),
    //         };
    //     }

    //     match tx.commit() {
    //         Ok(_) => log::debug!("Appointments successfully deleted"),
    //         Err(e) => log::error!("Couldn't delete appointments. Error: {:?}", e),
    //     }

    //     (appointments.len() as f64 / limit as f64).ceil() as usize
    // }

      /// Loads the locator associated to a given UUID
      pub(crate) async fn load_locator(&self, uuid: UUID) -> Option<Locator> {
        let key = uuid.to_vec();
        let mut tx = self.connection.begin().await.unwrap();
        let q = format!(
            "SELECT locator FROM appointments WHERE UUID=({})",
            uuid.to_vec()
        );
       let row = sqlx::query(&q)
       .fetch_one(&mut tx)
       .await;
       match row{
            Ok(row) => {
                let raw_locator: Vec<u8> = row.get(0).unwrap();
                Some(Locator::from_slice(&raw_locator).unwrap())
            }
            Err(_) => {
                None
            }
        }
    }

    /// Stores a [TransactionTracker] into the database.
    pub(crate) async fn store_tracker(
        &self,
        uuid: UUID,
        tracker: &TransactionTracker,
    ) -> Result<(), Error> {
        let (height, confirmed) = tracker.status.to_db_data().ok_or(Error::MissingField)?;
        let query =format!(
            "INSERT INTO trackers (UUID, dispute_tx, penalty_tx, height, confirmed) VALUES ({}, {}, {}, {}, {})",
            uuid.to_vec(),
            consensus::serialize(&tracker.dispute_tx),
            consensus::serialize(&tracker.penalty_tx),
            height,
            confirmed,
        );
        match self.store_data(&query).await{
            Ok(x) => {
                log::debug!("Tracker successfully stored: {}", uuid);
                Ok(x)
            }
            Err(e) => {
                log::error!("Couldn't store tracker: {}. Error: {:?}", uuid, e);
                Err(e)
            }
        }
    }


    /// Loads a [TransactionTracker] from the database.
    pub(crate) async fn load_tracker(&self, uuid: UUID) -> Option<TransactionTracker> {
        let key = uuid.to_vec();
        let mut tx = self.connection.begin().await.unwrap();
        let q = format!(
            "SELECT t.*, a.user_id FROM trackers as t INNER JOIN appointments as a ON t.UUID=a.UUID WHERE t.UUID=({})",
            key
        );
       let row = sqlx::query(&q)
       .fetch_one(&mut tx)
       .await;
       match row{
            Ok(row) => {
                let raw_dispute_tx: Vec<u8> = row.get(1).unwrap();
                let dispute_tx = consensus::deserialize(&raw_dispute_tx).unwrap();
                let raw_penalty_tx: Vec<u8> = row.get(2).unwrap();
                let penalty_tx = consensus::deserialize(&raw_penalty_tx).unwrap();
                let height: u32 = row.get(3).unwrap();
                let confirmed: bool = row.get(4).unwrap();
                let raw_userid: Vec<u8> = row.get(5).unwrap();
                let user_id = UserId::from_slice(&raw_userid).unwrap();

                return Some(TransactionTracker {
                    dispute_tx,
                    penalty_tx,
                    status: ConfirmationStatus::from_db_data(height, confirmed),
                    user_id,
                })
            }
            Err(_) => {
                None
            }
       }
    }

    /// Loads all trackers from the database.
    pub(crate) async fn load_all_trackers(&self) -> HashMap<UUID, TransactionTracker> {
        let mut trackers = HashMap::new();
        let mut tx = self.connection.begin().await.unwrap();
        let mut rows = sqlx::query("SELECT t.*, a.user_id FROM trackers as t INNER JOIN appointments as a ON t.UUID=a.UUID")
        .fetch(&mut tx);
        while let Some(row) = rows.try_next().await.unwrap() {
            let raw_uuid: Vec<u8> = row.get(0).unwrap();
            let uuid = UUID::from_slice(&raw_uuid[0..20]).unwrap();
            let raw_dispute_tx: Vec<u8> = row.get(1).unwrap();
            let dispute_tx = consensus::deserialize(&raw_dispute_tx).unwrap();
            let raw_penalty_tx: Vec<u8> = row.get(2).unwrap();
            let penalty_tx = consensus::deserialize(&raw_penalty_tx).unwrap();
            let height: u32 = row.get(3).unwrap();
            let confirmed: bool = row.get(4).unwrap();
            let raw_userid: Vec<u8> = row.get(5).unwrap();
            let user_id = UserId::from_slice(&raw_userid).unwrap();

            trackers.insert(
                uuid,
                TransactionTracker {
                    dispute_tx,
                    penalty_tx,
                    status: ConfirmationStatus::from_db_data(height, confirmed),
                    user_id,
                },
            );
        }

        trackers
    }

    /// Stores the last known block into the database.
    pub(crate) async fn store_last_known_block(&self, block_hash: &BlockHash) -> Result<(), Error> {
        let query = format!("INSERT OR REPLACE INTO last_known_block (id, block_hash) VALUES (0, {})",
                block_hash.to_vec()
    );
        match self.store_data(&query).await{
            Ok(x) => {
                println!("User successfully stored:");
                Ok(x)
            }
            Err(e) => {
                println!("Couldn't store user: Error: ");
                Err(e)
            }
        }
    }

    /// Loads the last known block from the database.
    pub async fn load_last_known_block(&self) -> Option<BlockHash> {
        let mut tx = self.connection.begin().await.unwrap();
        let q = format!(
            "SELECT block_hash FROM last_known_block WHERE id=0"
        );
       let row = sqlx::query(&q)
       .fetch_one(&mut tx)
       .await;
       match row{
            Ok(row) => {
                let raw_hash: Vec<u8> = row.get(0).unwrap();
                Ok(BlockHash::from_slice(&raw_hash).unwrap())
            }
            Err(_) => {
                None
            }
       }
       
    }

    /// Stores the tower secret key into the database.
    ///
    /// When a new key is generated, old keys are not overwritten but are not retrievable from the API either.
    pub async fn store_tower_key(&mut self, sk: i32) -> Result<(), Error> {
        let query = format!("INSERT INTO keys (key) VALUES ({})", sk);
        match self.store_data(&query).await {
            Ok(x) => {
                println!("User successfully stored:");
                Ok(x)
            }
            Err(e) => {
                println!("Couldn't store user: Error: ");
                Err(e)
            }
        }
    }

    // Loads the last known tower secret key from the database.
    //
    // Loads the key with higher id from the database. Old keys are not overwritten just in case a recovery is needed,
    // but they are not accessible from the API either.
    pub async fn load_tower_key(&mut self) -> Option<SecretKey> {
        let mut tx = self.connection.begin().await.unwrap();
        let q = format!(
            "SELECT key FROM keys WHERE id = (SELECT MAX(id) from keys)"
        );
        let keys  = sqlx::query(&q)
        .fetch_one(&mut tx)
        .await;
        match keys{
            Ok(keys) => {
                let sk:String = keys.get("key");
                Some(SecretKey::from_str(&sk).unwrap())
            }
            Err(_) =>{
                None
            }
        }
       
    }
}






#[async_std::main]
async fn main() -> Result<()>{
    let con1 = AnyConnection::connect("postgres://postgres:arsalan@localhost/postgres").await?;
    let con2 = AnyConnection::connect("sqlite::memory:").await?;
    let mut a    = DBM::new(con1, 1).await?;
    let p = a.load_tower_key().await;
  Ok(())
}