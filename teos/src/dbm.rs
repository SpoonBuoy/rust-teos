//! Logic related to the tower database manager (DBM), component in charge of persisting data on disk.
//!

use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::path::PathBuf;
use std::str::FromStr;


use std::sync::{Arc, Mutex};
use bitcoin::consensus::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::secp256k1::SecretKey;
use bitcoin::util::psbt::serialize::Serialize;
use bitcoin::{BlockHash, Transaction};


use teos_common::appointment::{Appointment, Locator, self};
use teos_common::constants::ENCRYPTED_BLOB_MAX_SIZE;
use teos_common::dbm::{DatabaseConnection, DatabaseManager, Error};
use teos_common::UserId;

use sqlx::{AnyConnection, Error as SqlxError, Connection, Executor, Row};

use crate::extended_appointment::{compute_appointment_slots, ExtendedAppointment, UUID};
use crate::gatekeeper::UserInfo;
use crate::responder::{ConfirmationStatus, TransactionTracker};

const TABLES: [&str; 5] = [
    "CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    available_slots INT NOT NULL,
    subscription_expiry INT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS appointments (
    UUID INT PRIMARY KEY,
    locator INT NOT NULL,
    encrypted_blob BLOB NOT NULL,
    to_self_delay INT NOT NULL,
    user_signature BLOB NOT NULL,
    start_block INT NOT NULL,
    user_id INT NOT NULL,
    FOREIGN KEY(user_id)
        REFERENCES users(user_id)
        ON DELETE CASCADE
)",
    "CREATE TABLE IF NOT EXISTS trackers (
    UUID INT PRIMARY KEY,
    dispute_tx BLOB NOT NULL,
    penalty_tx BLOB NOT NULL,
    height INT NOT NULL,
    confirmed BOOL NOT NULL,
    FOREIGN KEY(UUID)
        REFERENCES appointments(UUID)
        ON DELETE CASCADE
)",
    "CREATE TABLE IF NOT EXISTS last_known_block (
    id INT PRIMARY KEY,
    block_hash INT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key INT NOT NULL
)",
];

/// Component in charge of interacting with the underlying database.
///
/// Currently works for `SQLite`. `PostgreSQL` should also be added in the future.
#[derive(Debug)]
pub struct DBM {
    /// The underlying database connection.
    connection: AnyConnection,
}

impl DatabaseConnection for DBM {
    fn get_connection(&self) -> &AnyConnection {
        &self.connection
    }

    fn get_mut_connection(&mut self) -> &mut AnyConnection {
        &mut self.connection
    }
}

impl DBM {
    /// Creates a new [DBM] instance.
    pub async fn new(db_path: PathBuf) -> Result<Self, sqlx::Error> {
        let connection = AnyConnection::connect("sqlite::memory:").await?;
       // connection.execute("PRAGMA foreign_keys=1;", [])?;
        let mut dbm = Self { connection };
        dbm.create_tables(Vec::from_iter(TABLES)).await.unwrap();

        Ok(dbm)
    }

    /// Stores a user ([UserInfo]) into the database.
    pub(crate) async fn store_user(&mut self, user_id: UserId, user_info: &UserInfo) -> Result<(), Error> {
        let query_str =
            "INSERT INTO users (user_id, available_slots, subscription_expiry) VALUES ($1, $2, $3)";

        let query = sqlx::query::<sqlx::Any>(query_str)
            .bind(user_id.to_vec())
            .bind(user_info.available_slots as i32)
            .bind(user_info.subscription_expiry as i32);

        match self.store_data(
            query
        ).await{
            Ok(()) => {
                log::debug!("User successfully stored: {}", user_id);
                Ok(())
            }
            Err(e) => {
                log::error!("Couldn't store user: {}. Error: {:?}", user_id, e);
                Err(e)
            }
        }
    }

    /// Updates an existing user ([UserInfo]) in the database.
    pub(crate) async fn update_user(&mut self, user_id: UserId, user_info: &UserInfo) {
        let query_str =
            "UPDATE users SET available_slots=($1), subscription_expiry=($2) WHERE user_id=($3)";

        let query = sqlx::query::<sqlx::Any>(query_str)
            .bind(user_info.available_slots as i32)
            .bind(user_info.subscription_expiry as i32)
            .bind(user_id.to_vec());

        match self.update_data(
            query
        ).await {
            Ok(_) => {
                log::debug!("User's info successfully updated: {}", user_id);
            }
            Err(_) => {
                log::error!("User not found, data cannot be updated: {}", user_id);
            }
        }
    }

    /// Loads the associated appointments ([Appointment]) of a given user ([UserInfo]).
    pub(crate) async fn load_user_appointments(&mut self, user_id: UserId) -> HashMap<UUID, u32> {
        let mut tx = self.get_mut_connection().begin().await.unwrap();
        let query_str = "SELECT UUID, encrypted_blob FROM appointments WHERE user_id=(?)";
        let query = sqlx::query::<sqlx::Any>(query_str)
        .bind(user_id.to_vec());
        
        let rows = tx.fetch_all(query).await.unwrap();

        let mut appointments = HashMap::new();
        for row in rows{
            let raw_uuid: Vec<u8> = row.try_get(0).unwrap();
            let uuid = UUID::from_slice(&raw_uuid[0..20]).unwrap();
            let e_blob: Vec<u8> = row.try_get(1).unwrap();
    
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
      
        let mut tx = self.get_mut_connection().begin().await.unwrap();
        let query_str = "SELECT * FROM users";
        let query = sqlx::query::<sqlx::Any>(query_str);

        let rows = tx.fetch_all(query).await.unwrap();

        drop(tx);

        for row in rows{
            let raw_userid: Vec<u8> = row.try_get(0).unwrap();
            let user_id = UserId::from_slice(&raw_userid).unwrap();
            let slots:i32 = row.try_get(1).unwrap();
            let expiry:i32 = row.try_get(2).unwrap();
             
            users.insert(
                user_id,
                UserInfo::with_appointments(slots as u32, expiry as u32, self.load_user_appointments(user_id).await), 
            );
        }
        users
    }

    /// Removes some users from the database in batch.
    pub(crate) async fn batch_remove_users(&mut self, users: &HashSet<UserId>) -> usize {
        //let limit = self.connection.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER) as usize;
        let limit  = 8;
        let mut tx = self.get_mut_connection().begin().await.unwrap();
        let iter = users
            .iter()
            .map(|uuid| uuid.to_vec())
            .collect::<Vec<Vec<u8>>>();

        for chunk in iter.chunks(limit) {
            let query_str = "DELETE FROM users WHERE user_id IN ".to_owned();
            let placeholders = format!("(?{})", (", ?").repeat(chunk.len() - 1));
            let query_fmt = &format!("{}{}", query_str, placeholders);

            let mut query = sqlx::query::<sqlx::Any>(query_fmt);

            for i in chunk{
                query = query.bind(i);
            }

            match tx.execute(
                query
            ).await {
                Ok(_) => log::debug!("Users deletion added to db transaction"),
                Err(e) => log::error!("Couldn't add deletion query to transaction. Error: {:?}", e),
            }
        }

        match tx.commit().await{
            Ok(_) => log::debug!("Users successfully deleted"),
            Err(e) => log::error!("Couldn't delete users. Error: {:?}", e),
        }

        (users.len() as f64 / limit as f64).ceil() as usize
    }

    /// Stores an [Appointment] into the database.
    pub(crate) async fn store_appointment(
        &mut self,
        uuid: UUID,
        appointment: &ExtendedAppointment,
    ) -> Result<(), Error> {
        let query_str =
        "INSERT INTO appointments (UUID, locator, encrypted_blob, to_self_delay, user_signature, start_block, user_id) VALUES ($1, $2, $3, $4, $5, $6, $7)";

        let a = appointment.to_owned();

        let query = sqlx::query::<sqlx::Any>(query_str)
            .bind(uuid.to_vec())
            .bind(a.locator().to_vec())
            //.bind(a.encrypted_blob())
            .bind(a.to_self_delay() as i32)
            //.bind(&a.user_signature)
            .bind(a.start_block as i32)
            .bind(a.user_id.to_vec());

        match self.store_data(
            query
        ).await{
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
    pub(crate) async fn update_appointment(&mut self, uuid: UUID, appointment: &ExtendedAppointment) {
        // DISCUSS: Check what fields we'd like to make updatable. e_blob and signature are the obvious, to_self_delay and start_block may not be necessary (or even risky)
        let query_str =
        "UPDATE appointments SET encrypted_blob=($1), to_self_delay=($2), user_signature=($3), start_block=($4) WHERE UUID=($5)";

        let a = appointment.to_owned();

        let query = sqlx::query::<sqlx::Any>(query_str)
            //.bind(a.encrypted_blob())
            .bind(a.to_self_delay() as i32)
            //.bind(&a.user_signature)
            .bind(a.start_block as i32)
            .bind(uuid.to_vec());

        match self.update_data(
            query
        ).await {
            Ok(_) => {
                log::debug!("Appointment successfully updated: {}", uuid);
            }
            Err(_) => {
                log::error!("Appointment not found, data cannot be updated: {}", uuid);
            }
        }
    }

    /// Loads an [Appointment] from the database.
    pub(crate) async fn load_appointment(&mut self, uuid: UUID) -> Result<ExtendedAppointment, Error> {
        let key = uuid.to_vec();
        let mut tx = self.get_mut_connection().begin().await.unwrap();
        let query_str = "SELECT * FROM appointments WHERE UUID=($)";
        let query = sqlx::query::<sqlx::Any>(query_str).bind(key);

        let row = tx.fetch_one(query).await;
        match row{
            Ok(row) => {
                let raw_locator: Vec<u8> = row.try_get(1).unwrap();
                let locator = Locator::from_slice(&raw_locator).unwrap();
                let raw_userid: Vec<u8> = row.try_get(6).unwrap();
                let raw_locator: Vec<u8> = row.try_get(1).unwrap();
                let locator = Locator::from_slice(&raw_locator).unwrap();
                let raw_userid: Vec<u8> = row.try_get(6).unwrap();
                let user_id = UserId::from_slice(&raw_userid).unwrap();

                let appointment = Appointment::new(locator, row.try_get(2).unwrap(), row.try_get(3).unwrap_or(0) as u32);
                Ok(ExtendedAppointment::new(
                    appointment,
                    user_id,
                    row.try_get(4).unwrap(),
                    row.try_get(5).unwrap_or(0) as u32,
                ))
            }
            Err(_) => Err(Error::NotFound)
        }

    }

    /// Loads all appointments from the database.
    pub(crate) async fn load_all_appointments(&mut self) -> HashMap<UUID, ExtendedAppointment> {
        let mut appointments = HashMap::new();
        let mut tx = self.get_mut_connection().begin().await.unwrap();
        let query_str = "SELECT * FROM appointments as a LEFT JOIN trackers as t ON a.UUID=t.UUID WHERE t.UUID IS NULL";
        let query = sqlx::query::<sqlx::Any>(query_str);
        
        let rows = tx.fetch_all(query).await.unwrap();
        for row in rows {
            let raw_uuid: Vec<u8> = row.try_get(0).unwrap();
            let uuid = UUID::from_slice(&raw_uuid[0..20]).unwrap();
            let raw_locator: Vec<u8> = row.try_get(1).unwrap();
            let locator = Locator::from_slice(&raw_locator).unwrap();
            let raw_userid: Vec<u8> = row.try_get(6).unwrap();
            let user_id = UserId::from_slice(&raw_userid).unwrap();

            let appointment = Appointment::new(locator, row.try_get(2).unwrap(), row.try_get(3).unwrap_or(0) as u32);

            appointments.insert(
                uuid,
                ExtendedAppointment::new(
                    appointment,
                    user_id,
                    row.try_get(4).unwrap(),
                    row.try_get(5).unwrap_or(0) as u32,
                ),
            );
        }

        appointments
    }

    /// Removes an [Appointment] from the database.
    pub(crate) async fn remove_appointment(&mut self, uuid: UUID) {
        let query_str = "DELETE FROM appointments WHERE UUID=($1)";
        let query = sqlx::query::<sqlx::Any>(query_str).bind(uuid.to_vec());
        match self.remove_data(query).await {
            Ok(_) => {
                log::debug!("Appointment successfully removed: {}", uuid);
            }
            Err(_) => {
                log::error!("Appointment not found, data cannot be removed: {}", uuid);
            }
        }
    }

    /// Removes some appointments from the database in batch and updates the associated users giving back
    /// the freed appointment slots
    pub(crate) async fn batch_remove_appointments(
        &mut self,
        appointments: &HashSet<UUID>,
        updated_users: &HashMap<UserId, UserInfo>,
    ) -> usize {
        let limit = 8;
        let mut tx = self.get_mut_connection().begin().await.unwrap();
        let iter = appointments
            .iter()
            .map(|uuid| uuid.to_vec())
            .collect::<Vec<Vec<u8>>>();

        for chunk in iter.chunks(limit) {
            let query_str = "DELETE FROM appointments WHERE UUID IN ".to_owned();
            let placeholders = format!("(?{})", (", ?").repeat(chunk.len() - 1));
            let query_fmt = &format!("{}{}", query_str, placeholders);

            let mut query = sqlx::query::<sqlx::Any>(query_fmt);

            for i in chunk{
                query = query.bind(i);
            }

            match tx.execute(
                query
            ).await {
                Ok(_) => log::debug!("Appointments deletion added to db transaction"),
                Err(e) => log::error!("Couldn't add deletion query to transaction. Error: {:?}", e),
            }
        }

        for (id, info) in updated_users.iter() {
            let query_str = "UPDATE users SET available_slots=($1) WHERE user_id=($2)";
            let query = sqlx::query::<sqlx::Any>(query_str)
                .bind(info.available_slots as i32)
                .bind(id.to_vec());
            match tx.execute(query).await {
                Ok(_) => log::debug!("User update added to db transaction"),
                Err(e) => log::error!("Couldn't add update query to transaction. Error: {:?}", e),
            };
        }

        match tx.commit().await{
            Ok(_) => log::debug!("Appointments successfully deleted"),
            Err(e) => log::error!("Couldn't delete appointments. Error: {:?}", e),
        }

        (appointments.len() as f64 / limit as f64).ceil() as usize
    }

    /// Loads the locator associated to a given UUID
    pub(crate) async fn load_locator(&mut self, uuid: UUID) -> Result<Locator, Error> {
        let mut tx = self.get_mut_connection().begin().await.unwrap();
        let query_str = "SELECT locator FROM appointments WHERE UUID=($1)";
        let query = sqlx::query::<sqlx::Any>(query_str).bind(uuid.to_vec());

        let row = tx.fetch_one(query).await;
        
        match row {
            Ok(row) => {
                let raw_locator: Vec<u8> = row.try_get(0).unwrap();
                Ok(Locator::from_slice(&raw_locator).unwrap())
            }
            Err(_) => Err(Error::NotFound)
        }
    }

    /// Stores a [TransactionTracker] into the database.
    pub(crate) async fn store_tracker(
        &mut self,
        uuid: UUID,
        tracker: &TransactionTracker,
    ) -> Result<(), Error> {
        let (height, confirmed) = tracker.status.to_db_data().ok_or(Error::MissingField)?;

        let query_str =
        "INSERT INTO trackers (UUID, dispute_tx, penalty_tx, height, confirmed) VALUES ($1, $2, $3, $4, $5)";

        let query = sqlx::query::<sqlx::Any>(query_str)
            .bind(uuid.to_vec())
            .bind(tracker.dispute_tx.serialize())
            .bind(tracker.penalty_tx.serialize())
            .bind(tracker.penalty_tx.serialize())
            .bind(height as i32)
            .bind(confirmed);

        match self.store_data(
            query
        ).await {
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
    pub(crate) async fn load_tracker(&mut self, uuid: UUID) -> Result<TransactionTracker, Error> {
        let key = uuid.to_vec();
        let mut tx = self.get_mut_connection().begin().await.unwrap();
        let query_str = "SELECT t.*, a.user_id FROM trackers as t INNER JOIN appointments as a ON t.UUID=a.UUID WHERE t.UUID=($1)";
        let query = sqlx::query::<sqlx::Any>(query_str).bind(key);

        let row = tx.fetch_one(query).await;
        
        match row {
            Ok(row) => {
                let raw_dispute_tx: Vec<u8> = row.try_get(1).unwrap();
                let dispute_tx = deserialize::<Transaction>(&raw_dispute_tx).unwrap();
                let raw_penalty_tx: Vec<u8> = row.try_get(2).unwrap();
                let penalty_tx = deserialize::<Transaction>(&raw_penalty_tx).unwrap();
                let height: u32 = row.try_get(3).unwrap_or(0) as u32;
                let confirmed: bool = row.try_get(4).unwrap();
                let raw_userid: Vec<u8> = row.try_get(5).unwrap();
                let user_id = UserId::from_slice(&raw_userid).unwrap();

                Ok(TransactionTracker {
                    dispute_tx,
                    penalty_tx,
                    status: ConfirmationStatus::from_db_data(height, confirmed),
                    user_id,
                })
            }
            Err(_) => Err(Error::NotFound)   
        }
    }

    /// Loads all trackers from the database.
    pub(crate) async fn load_all_trackers(&mut self) -> HashMap<UUID, TransactionTracker> {
        let mut trackers = HashMap::new();
        let mut tx = self.get_mut_connection().begin().await.unwrap();
        let query_str = "SELECT t.*, a.user_id FROM trackers as t INNER JOIN appointments as a ON t.UUID=a.UUID";
        let query = sqlx::query::<sqlx::Any>(query_str);

        let rows = tx.fetch_all(query).await.unwrap();
     
        for row in rows {
            let raw_uuid: Vec<u8> = row.try_get(0).unwrap();
            let uuid = UUID::from_slice(&raw_uuid[0..20]).unwrap();
            let raw_dispute_tx: Vec<u8> = row.try_get(1).unwrap();
            let dispute_tx = deserialize::<Transaction>(&raw_dispute_tx).unwrap();
            let raw_penalty_tx: Vec<u8> = row.try_get(2).unwrap();
            let penalty_tx = deserialize::<Transaction>(&raw_penalty_tx).unwrap();
            let height: u32 = row.try_get(3).unwrap_or(0) as u32;
            let confirmed: bool = row.try_get(4).unwrap();
            let raw_userid: Vec<u8> = row.try_get(5).unwrap();
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
    pub(crate) async fn store_last_known_block(&mut self, block_hash: &BlockHash) -> Result<(), Error> {
        let query_str = "INSERT OR REPLACE INTO last_known_block (id, block_hash) VALUES (0, $1)";
        let query = sqlx::query::<sqlx::Any>(query_str).bind(block_hash.to_vec());
        self.store_data(query).await
    }

    /// Loads the last known block from the database.
    pub async fn load_last_known_block(&mut self) -> Result<BlockHash, Error> {
        let mut tx = self.get_mut_connection().begin().await.unwrap();
        let query_str = "SELECT block_hash FROM last_known_block WHERE id=0";
        let query = sqlx::query::<sqlx::Any>(query_str);

        let row = tx.fetch_one(query).await;
        match row {
            Ok(row) => {
                let raw_hash: Vec<u8> = row.try_get(0).unwrap();
                Ok(BlockHash::from_slice(&raw_hash).unwrap())
            }
            Err(_) => Err(Error::NotFound)
        }
    }

    /// Stores the tower secret key into the database.
    ///
    /// When a new key is generated, old keys are not overwritten but are not retrievable from the API either.
    pub async fn store_tower_key(&mut self, sk: &SecretKey) -> Result<(), Error> {
        let query_str = "INSERT INTO keys (key) VALUES (?)";
        let query = sqlx::query::<sqlx::Any>(query_str).bind(sk.to_string());
        self.store_data(query).await
    }

    /// Loads the last known tower secret key from the database.
    ///
    /// Loads the key with higher id from the database. Old keys are not overwritten just in case a recovery is needed,
    /// but they are not accessible from the API either.
    pub async fn load_tower_key(&mut self) -> Result<SecretKey, Error> {
        let mut tx = self.get_mut_connection().begin().await.unwrap();
        let query_str = "SELECT key FROM keys WHERE id = (SELECT seq FROM sqlite_sequence WHERE name=($1))";
        let query = sqlx::query::<sqlx::Any>(query_str).bind("keys");

        let row = tx.fetch_one(query).await;
        match row {
            Ok(row) => {
                let sk: String = row.try_get(0).unwrap();
                Ok(SecretKey::from_str(&sk).unwrap())
            }
            Err(_) => Err(Error::NotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_utils::{
        generate_dummy_appointment, generate_dummy_appointment_with_user, generate_uuid,
        get_random_tracker, get_random_user_id,
    };
    use std::iter::FromIterator;
    use teos_common::cryptography::get_random_bytes;

    impl DBM {
        pub(crate) async fn in_memory() -> Result<Self, SqlxError> {
            let connection =AnyConnection::connect("sqlite::memory:").await?;
           // connection.execute("PRAGMA foreign_keys=1;", [])?;
            let mut dbm = Self { connection };
            dbm.create_tables(Vec::from_iter(TABLES)).await.unwrap();

            Ok(dbm)
        }

        pub(crate) async fn load_user(&mut self, user_id: UserId) -> Result<UserInfo, Error> {
            let key = user_id.to_vec();
            let mut tx = self.get_mut_connection().begin().await.unwrap();
            let query_str = "SELECT available_slots, subscription_expiry FROM users WHERE user_id=($1)";
            let query = sqlx::query::<sqlx::Any>(query_str).bind(key);

            let row = tx.fetch_one(query).await;
            drop(tx);
            let user = match row{
                    Ok(row) => {
                        let slots = row.try_get(0).unwrap_or(0) as u32;
                        let expiry = row.try_get(1).unwrap_or(0) as u32;
                        Ok(UserInfo::with_appointments(
                        slots,
                        expiry,
                        self.load_user_appointments(user_id).await,
                        ))
                    }
                    Err(_) => Err(Error::NotFound)
                };
            user
        }
    }
    #[tokio::test]
    async fn test_create_tables() {
        let connection =AnyConnection::connect("sqlite::memory:").await.unwrap();
        let mut dbm = DBM { connection };
        dbm.create_tables(Vec::from_iter(TABLES)).await.unwrap();
    }

    #[tokio::test]
    async fn test_store_load_user() {
        let mut dbm = DBM::in_memory().await.unwrap();
       

        let user_id = get_random_user_id();
        let mut user = UserInfo::new(21, 42);

        

        assert!(matches!(dbm.store_user(user_id, &user).await, Ok { .. }));
        assert_eq!(dbm.load_user(user_id).await.unwrap(), user);

        // User info should be updatable but only via the update_user method
        user = UserInfo::new(42, 21);
        assert!(matches!(
            dbm.store_user(user_id, &user).await,
            Err{ .. }
            //This should be the actual error : (Error::AlreadyExists)
        ));
    }

    #[tokio::test]
    async fn test_store_load_user_with_appointments() {
        let mut dbm = DBM::in_memory().await.unwrap();

        let user_id = get_random_user_id();
        let mut user = UserInfo::new(21, 42);

        dbm.store_user(user_id, &user).await.unwrap();

        // Add some appointments to the user
        for _ in 0..10 {
            let (uuid, appointment) = generate_dummy_appointment_with_user(user_id, None);
            dbm.store_appointment(uuid, &appointment).await.unwrap();
            user.appointments.insert(uuid, 1);
        }

        // Check both loading the whole user info or only the associated appointments
        assert_eq!(dbm.load_user(user_id).await.unwrap(), user);
        assert_eq!(dbm.load_user_appointments(user_id).await, user.appointments);
    }

    #[tokio::test]
    async fn test_load_nonexistent_user() {
        let mut dbm = DBM::in_memory().await.unwrap();

        let user_id = get_random_user_id();
        assert!(matches!(dbm.load_user(user_id).await, Err(Error::NotFound)));
    }

    #[tokio::test]
    async fn test_update_user() {
        let mut dbm = DBM::in_memory().await.unwrap();

        let user_id = get_random_user_id();
        let mut user = UserInfo::new(21, 42);

        dbm.store_user(user_id, &user).await.unwrap();
        assert_eq!(dbm.load_user(user_id).await.unwrap(), user);

        user.available_slots *= 2;
        dbm.update_user(user_id, &user).await;
        assert_eq!(dbm.load_user(user_id).await.unwrap(), user);
    }

    #[tokio::test]
    async fn test_load_all_users() {
        let mut dbm = DBM::in_memory().await.unwrap();
        let mut users = HashMap::new();

        for i in 1..11 {
            let user_id = get_random_user_id();
            let user = UserInfo::new(i, i * 2);
            users.insert(user_id, user.clone());
            dbm.store_user(user_id, &user).await.unwrap();

            // Add appointments to some of the users
            if i % 2 == 0 {
                let (uuid, appointment) = generate_dummy_appointment_with_user(user_id, None);
                dbm.store_appointment(uuid, &appointment).await.unwrap();
                users
                    .get_mut(&user_id)
                    .unwrap()
                    .appointments
                    .insert(uuid, 1);
            }
        }

        assert_eq!(dbm.load_all_users().await, users);
    }

    #[tokio::test]
    async fn test_batch_remove_users() {
        let mut dbm = DBM::in_memory().await.unwrap();

        // Set a limit value for the maximum number of variables in SQLite so we can
        // test splitting big queries into chunks.
        let limit = 10;
        // dbm.connection
        //     .set_limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER, limit);

        let mut to_be_deleted = HashSet::new();
        let mut rest = HashSet::new();
        for i in 1..100 {
            let user_id = get_random_user_id();
            let user = UserInfo::new(21, 42);
            dbm.store_user(user_id, &user).await.unwrap();

            if i % 2 == 0 {
                to_be_deleted.insert(user_id);
            } else {
                rest.insert(user_id);
            }
        }

        // Check that the db transaction had 5 (100/2*10) queries on it
        assert_eq!(dbm.batch_remove_users(&to_be_deleted).await, 5);
        // Check user data was deleted
        assert_eq!(
            rest,
            dbm.load_all_users()
                .await
                .keys()
                .cloned()
                .collect::<HashSet<UserId>>()
        );
    }

    #[tokio::test]
    async fn test_batch_remove_users_cascade() {
        // Test that removing users cascade deleted appointments and trackers
        let mut dbm = DBM::in_memory().await.unwrap();
        let uuid = generate_uuid();
        let appointment = generate_dummy_appointment(None);
        // The confirmation status doesn't really matter here, it can be any of {ConfirmedIn, InMempoolSince}.
        let tracker = get_random_tracker(appointment.user_id, ConfirmationStatus::ConfirmedIn(100));

        // Add the user and link an appointment (this is usually done once the appointment)
        // is added after the user creation, but for the test purpose it can be done all at once.
        let info = UserInfo::new(21, 42);
        dbm.store_user(appointment.user_id, &info).await.unwrap();

        // Appointment only
        assert!(matches!(
            dbm.store_appointment(uuid, &appointment).await,
            Ok { .. }
        ));

        dbm.batch_remove_users(&HashSet::from_iter(vec![appointment.user_id])).await;
        assert!(matches!(
            dbm.load_user(appointment.user_id).await,
            Err(Error::NotFound)
        ));
        assert!(matches!(dbm.load_appointment(uuid).await, Err(Error::NotFound)));

        // Appointment + Tracker
        dbm.store_user(appointment.user_id, &info).await.unwrap();
        assert!(matches!(
            dbm.store_appointment(uuid, &appointment).await,
            Ok { .. }
        ));
        assert!(matches!(dbm.store_tracker(uuid, &tracker).await, Ok { .. }));

        dbm.batch_remove_users(&HashSet::from_iter(vec![appointment.user_id])).await;
        assert!(matches!(
            dbm.load_user(appointment.user_id).await,
            Err(Error::NotFound)
        ));
        assert!(matches!(dbm.load_appointment(uuid).await, Err(Error::NotFound)));
        assert!(matches!(dbm.load_tracker(uuid).await, Err(Error::NotFound)));
    }

    #[tokio::test]
    async fn test_batch_remove_nonexistent_users() {
        let mut dbm = DBM::in_memory().await.unwrap();
        let users = (0..10)
            .map(|_| get_random_user_id())
            .collect::<HashSet<UserId>>();

        // Test it does not fail even if the user does not exist (it will log though)
        dbm.batch_remove_users(&users).await;
    }

    #[tokio::test]
    async fn test_store_load_appointment() {
        let mut dbm = DBM::in_memory().await.unwrap();

        // In order to add an appointment we need the associated user to be present
        let user_id = get_random_user_id();
        let user = UserInfo::new(21, 42);
        dbm.store_user(user_id, &user).await.unwrap();

        let (uuid, appointment) = generate_dummy_appointment_with_user(user_id, None);

        assert!(matches!(
            dbm.store_appointment(uuid, &appointment).await,
            Ok { .. }
        ));
        assert_eq!(dbm.load_appointment(uuid).await.unwrap(), appointment);

        // Appointment info should be updatable but only via the update_appointment method
        assert!(matches!(
            dbm.store_appointment(uuid, &appointment).await,
            Err(Error::AlreadyExists)
        ));
    }

    #[tokio::test]
    async fn test_store_appointment_missing_user() {
        let mut dbm = DBM::in_memory().await.unwrap();

        let uuid = generate_uuid();
        let appointment = generate_dummy_appointment(None);

        assert!(matches!(
            dbm.store_appointment(uuid, &appointment).await,
            Err(Error::MissingForeignKey)
        ));
        assert!(matches!(dbm.load_tracker(uuid).await, Err(Error::NotFound)));
    }

    #[tokio::test]
    async fn test_load_nonexistent_appointment() {
        let mut dbm = DBM::in_memory().await.unwrap();

        let uuid = generate_uuid();
        assert!(matches!(dbm.load_appointment(uuid).await, Err(Error::NotFound)));
    }

    #[tokio::test]
    async fn test_update_appointment() {
        let mut dbm = DBM::in_memory().await.unwrap();

        let user_id = get_random_user_id();
        let user = UserInfo::new(21, 42);
        dbm.store_user(user_id, &user).await.unwrap();

        let (uuid, appointment) = generate_dummy_appointment_with_user(user_id, None);
        assert!(matches!(
            dbm.store_appointment(uuid, &appointment).await,
            Ok { .. }
        ));

        // Modify the appointment and update it
        let mut modified_appointment = appointment;
        modified_appointment.inner.encrypted_blob.reverse();

        // Not all fields are updatable, create another appointment modifying fields that cannot be
        let mut another_modified_appointment = modified_appointment.clone();
        another_modified_appointment.user_id = get_random_user_id();

        // Check how only the modifiable fields have been updated
        dbm.update_appointment(uuid, &another_modified_appointment).await;
        assert_eq!(dbm.load_appointment(uuid).await.unwrap(), modified_appointment);
        assert_ne!(
            dbm.load_appointment(uuid).await.unwrap(),
            another_modified_appointment
        );
    }

    #[tokio::test]
    async fn test_load_all_appointments() {
        let mut dbm = DBM::in_memory().await.unwrap();
        let mut appointments = HashMap::new();

        for i in 1..11 {
            let user_id = get_random_user_id();
            let user = UserInfo::new(i, i * 2);
            dbm.store_user(user_id, &user).await.unwrap();

            let (uuid, appointment) = generate_dummy_appointment_with_user(user_id, None);
            dbm.store_appointment(uuid, &appointment).await.unwrap();
            appointments.insert(uuid, appointment);
        }

        assert_eq!(dbm.load_all_appointments().await, appointments);

        // If an appointment has an associated tracker, it should not be loaded since it is seen
        // as a triggered appointment
        let user_id = get_random_user_id();
        let user = UserInfo::new(21, 42);
        dbm.store_user(user_id, &user).await.unwrap();

        let (uuid, appointment) = generate_dummy_appointment_with_user(user_id, None);
        dbm.store_appointment(uuid, &appointment).await.unwrap();

        // The confirmation status doesn't really matter here, it can be any of {ConfirmedIn, InMempoolSince}.
        let tracker = get_random_tracker(user_id, ConfirmationStatus::InMempoolSince(100));
        dbm.store_tracker(uuid, &tracker).await.unwrap();

        // We should get all the appointments back except from the triggered one
        assert_eq!(dbm.load_all_appointments().await, appointments);
    }

    #[tokio::test]
    async fn test_batch_remove_appointments() {
        let mut dbm = DBM::in_memory().await.unwrap();

        // Set a limit value for the maximum number of variables in SQLite so we can
        // test splitting big queries into chunks.
        let limit = 10;
        // dbm.connection
        //     .set_limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER, limit);

        let user_id = get_random_user_id();
        let mut user = UserInfo::new(500, 42);
        dbm.store_user(user_id, &user).await.unwrap();

        let mut rest = HashSet::new();
        for i in 1..6 {
            let mut to_be_deleted = HashSet::new();
            for j in 0..limit * 2 * i {
                let (uuid, appointment) = generate_dummy_appointment_with_user(user_id, None);
                dbm.store_appointment(uuid, &appointment).await.unwrap();

                if j % 2 == 0 {
                    to_be_deleted.insert(uuid);
                } else {
                    rest.insert(uuid);
                }
            }

            // When the appointment are deleted, the user will get back slots based on the deleted data.
            // Here we can just make a number up to make sure it matches.
            user.available_slots = i as u32;
            let updated_users = HashMap::from_iter([(user_id, user.clone())]);

            // Check that the db transaction had i queries on it
            assert_eq!(
                dbm.batch_remove_appointments(&to_be_deleted, &updated_users).await,
                i as usize
            );
            // Check appointment data was deleted and users properly updated
            assert_eq!(
                rest,
                dbm.load_all_appointments()
                    .await
                    .keys()
                    .cloned()
                    .collect::<HashSet<UUID>>()
            );
            assert_eq!(
                dbm.load_user(user_id).await.unwrap().available_slots,
                user.available_slots
            );
        }
    }

    #[tokio::test]
    async fn test_batch_remove_appointments_cascade() {
        let mut dbm = DBM::in_memory().await.unwrap();
        let uuid = generate_uuid();
        let appointment = generate_dummy_appointment(None);
        // The confirmation status doesn't really matter here, it can be any of {ConfirmedIn, InMempoolSince}.
        let tracker = get_random_tracker(appointment.user_id, ConfirmationStatus::ConfirmedIn(21));

        let info = UserInfo::new(21, 42);

        // Add the user b/c of FK restrictions
        dbm.store_user(appointment.user_id, &info).await.unwrap();

        // Appointment only
        assert!(matches!(
            dbm.store_appointment(uuid, &appointment).await,
            Ok { .. }
        ));

        dbm.batch_remove_appointments(
            &HashSet::from_iter(vec![uuid]),
            &HashMap::from_iter([(appointment.user_id, info.clone())]),
        ).await;
        assert!(matches!(dbm.load_appointment(uuid).await, Err(Error::NotFound)));

        // Appointment + Tracker
        assert!(matches!(
            dbm.store_appointment(uuid, &appointment).await,
            Ok { .. }
        ));
        assert!(matches!(dbm.store_tracker(uuid, &tracker).await, Ok { .. }));

        dbm.batch_remove_appointments(
            &HashSet::from_iter(vec![uuid]),
            &HashMap::from_iter([(appointment.user_id, info)]),
        ).await;
        assert!(matches!(dbm.load_appointment(uuid).await, Err(Error::NotFound)));
        assert!(matches!(dbm.load_tracker(uuid).await, Err(Error::NotFound)));
    }

    #[tokio::test]
    async fn test_batch_remove_nonexistent_appointments() {
        let mut dbm = DBM::in_memory().await.unwrap();
        let appointments = (0..10).map(|_| generate_uuid()).collect::<HashSet<UUID>>();

        // Test it does not fail even if the user does not exist (it will log though)
        dbm.batch_remove_appointments(&appointments, &HashMap::new()).await;
    }
    #[tokio::test]
    async fn test_load_locator() {
        let mut dbm = DBM::in_memory().await.unwrap();

        // In order to add an appointment we need the associated user to be present
        let user_id = get_random_user_id();
        let user = UserInfo::new(21, 42);
        dbm.store_user(user_id, &user).await.unwrap();

        let (uuid, appointment) = generate_dummy_appointment_with_user(user_id, None);

        assert!(matches!(
            dbm.store_appointment(uuid, &appointment).await,
            Ok { .. }
        ));

        // We should be able to load the locator now the appointment exists
        assert_eq!(dbm.load_locator(uuid).await.unwrap(), appointment.locator());
    }

    #[tokio::test]
    async fn test_load_nonexistent_locator() {
        let mut dbm = DBM::in_memory().await.unwrap();

        let (uuid, _) = generate_dummy_appointment_with_user(get_random_user_id(), None);
        assert!(matches!(dbm.load_locator(uuid).await, Err(Error::NotFound)));
    }

    #[tokio::test]
    async fn test_store_load_tracker() {
        let mut dbm = DBM::in_memory().await.unwrap();

        // In order to add a tracker we need the associated appointment to be present (which
        // at the same time requires an associated user to be present)
        let user_id = get_random_user_id();
        let user = UserInfo::new(21, 42);
        dbm.store_user(user_id, &user).await.unwrap();

        let (uuid, appointment) = generate_dummy_appointment_with_user(user_id, None);
        dbm.store_appointment(uuid, &appointment).await.unwrap();

        // The confirmation status doesn't really matter here, it can be any of {ConfirmedIn, InMempoolSince}.
        let tracker = get_random_tracker(user_id, ConfirmationStatus::ConfirmedIn(21));
        assert!(matches!(dbm.store_tracker(uuid, &tracker).await, Ok { .. }));
        assert_eq!(dbm.load_tracker(uuid).await.unwrap(), tracker);
    }

    #[tokio::test]
    async fn test_store_duplicate_tracker() {
        let mut dbm = DBM::in_memory().await.unwrap();

        let user_id = get_random_user_id();
        let user = UserInfo::new(21, 42);
        dbm.store_user(user_id, &user).await.unwrap();

        let (uuid, appointment) = generate_dummy_appointment_with_user(user_id, None);
        dbm.store_appointment(uuid, &appointment).await.unwrap();

        // The confirmation status doesn't really matter here, it can be any of {ConfirmedIn, InMempoolSince}.
        let tracker = get_random_tracker(user_id, ConfirmationStatus::InMempoolSince(42));
        assert!(matches!(dbm.store_tracker(uuid, &tracker).await, Ok { .. }));

        // Try to store it again, but it shouldn't go through
        assert!(matches!(
            dbm.store_tracker(uuid, &tracker).await,
            Err(Error::AlreadyExists)
        ));
    }

    #[tokio::test]
    async fn test_store_tracker_missing_appointment() {
        let mut dbm = DBM::in_memory().await.unwrap();

        let uuid = generate_uuid();
        let user_id = get_random_user_id();

        // The confirmation status doesn't really matter here, it can be any of {ConfirmedIn, InMempoolSince}.
        let tracker = get_random_tracker(user_id, ConfirmationStatus::InMempoolSince(42));

        assert!(matches!(
            dbm.store_tracker(uuid, &tracker).await,
            Err(Error::MissingForeignKey)
        ));
    }

    #[tokio::test]
    async fn test_load_nonexistent_tracker() {
        let mut dbm = DBM::in_memory().await.unwrap();

        let uuid = generate_uuid();
        assert!(matches!(dbm.load_tracker(uuid).await, Err(Error::NotFound)));
    }

    #[tokio::test]
    async fn test_load_all_trackers() {
        let mut dbm = DBM::in_memory().await.unwrap();
        let mut trackers = HashMap::new();

        for i in 1..11 {
            let user_id = get_random_user_id();
            let user = UserInfo::new(i, i * 2);
            dbm.store_user(user_id, &user).await.unwrap();

            let (uuid, appointment) = generate_dummy_appointment_with_user(user_id, None);
            dbm.store_appointment(uuid, &appointment).await.unwrap();

            // The confirmation status doesn't really matter here, it can be any of {ConfirmedIn, InMempoolSince}.
            let tracker = get_random_tracker(user_id, ConfirmationStatus::InMempoolSince(42));
            dbm.store_tracker(uuid, &tracker).await.unwrap();
            trackers.insert(uuid, tracker);
        }

        assert_eq!(dbm.load_all_trackers().await, trackers);
    }

    #[tokio::test]
    async fn test_store_load_last_known_block() {
        let mut dbm = DBM::in_memory().await.unwrap();

        let mut block_hash = BlockHash::from_slice(&get_random_bytes(32)).unwrap();
        dbm.store_last_known_block(&block_hash).await.unwrap();
        assert_eq!(dbm.load_last_known_block().await.unwrap(), block_hash);

        // Update with a new hash to check it can be done
        block_hash = BlockHash::from_slice(&get_random_bytes(32)).unwrap();
        dbm.store_last_known_block(&block_hash).await.unwrap();
        assert_eq!(dbm.load_last_known_block().await.unwrap(), block_hash);
    }

    #[tokio::test]
    async fn test_store_load_nonexistent_last_known_block() {
        let mut dbm = DBM::in_memory().await.unwrap();

        assert!(matches!(dbm.load_last_known_block().await, Err(Error::NotFound)));
    }
}
