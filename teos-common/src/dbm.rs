use async_trait::async_trait;
use anyhow::*;
use sqlx::{AnyConnection, Error as SqlxError, Connection, Executor, query::Query};
use std::result::Result::Ok;
#[derive(Debug)]
pub enum Error {
    AlreadyExists,
    MissingForeignKey,
    MissingField,
    NotFound,
    Unknown(SqlxError),
}
#[async_trait]
pub trait DatabaseConnection{
    fn get_connection(&self) -> &AnyConnection;

    fn get_mut_connection(&mut self) -> &mut AnyConnection;

}
#[async_trait]
pub trait DatabaseManager: Sized {
    async fn create_tables(&mut self, tables: Vec<&str>) -> Result<()>;
    async fn store_data(&mut self, quer: Query<'static, sqlx::Any, sqlx::any::AnyArguments<'static>>) -> Result<(), Error>;
    async fn remove_data(&mut self, quer: Query<'static, sqlx::Any, sqlx::any::AnyArguments<'static>>) -> Result<(), Error>;
    async fn update_data(&mut self, quer: Query<'static, sqlx::Any, sqlx::any::AnyArguments<'static>>) -> Result<(), Error>;
 }

#[async_trait]
impl <T:DatabaseConnection + std::marker::Send> DatabaseManager for T{
    /// Creates the database tables if not present.
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
   /// Generic method to store data into the database.
   async fn store_data(
         &mut self, 
         quer: Query<'static, sqlx::Any, sqlx::any::AnyArguments<'static>>
    ) -> Result<(), Error>{
       let mut tx = self.get_mut_connection().begin().await.unwrap();
       let res = quer
                .execute(&mut tx)
                .await;
        match res{
            Ok(_) =>{
                tx.commit().await.unwrap();   
                Ok(())
            }
            Err(e) => Err(Error::Unknown(e))
        }
   }
   /// Generic method to remove data from the database.
   async fn remove_data(
        &mut self, 
        quer: Query<'static, sqlx::Any, sqlx::any::AnyArguments<'static>>
    ) -> Result<(), Error>{
        let mut tx = self.get_mut_connection().begin().await.unwrap();
        let res = quer.
                    execute(&mut tx)
                    .await.unwrap().rows_affected();
        //Error checking block to be implemented
        match res{
            0 => Err(Error::Unknown(SqlxError::RowNotFound)),
            _ =>{
                tx.commit().await.unwrap(); 
                Ok(())
            }
        } 
   }
   ///Generic method to update data from the database
   async fn update_data(
        &mut self, 
        quer: Query<'static, sqlx::Any, sqlx::any::AnyArguments<'static>>
    ) -> Result<(), Error>{
        // Updating data is fundamentally the same as deleting it in terms of interface.
        // A query is sent and either no row is modified or some rows are
        self.remove_data(quer).await
   }

}