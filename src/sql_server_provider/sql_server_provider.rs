use crate::config_provider::SourceDatabase;
use crate::version::PRODUCT_NAME;
use anyhow::Result;
use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use futures::TryStreamExt;
use futures::future::join_all;
use tiberius::{AuthMethod, ColumnType, Config, EncryptionLevel, QueryItem};
use tokio::task::JoinHandle;

pub struct SqlServerProvider {
    config: Config,
}

impl SqlServerProvider {
    pub fn new(source_database: &SourceDatabase) -> Self {
        let mut config = Config::new();
        config.host(source_database.get_host_as_ref());
        config.port(source_database.get_port_as_ref().clone());
        config.database(source_database.get_database_as_ref());
        config.authentication(AuthMethod::sql_server(
            source_database.get_user_as_ref(),
            source_database.get_password_as_ref(),
        ));
        config.trust_cert();
        config.readonly(true);
        config.application_name(PRODUCT_NAME);
        config.encryption(EncryptionLevel::NotSupported); // TODO: remove on PROD
        SqlServerProvider { config }
    }

    pub async fn sql_server_test(&self) -> Result<()> {
        let manager = ConnectionManager::new(self.config.clone());
        let pool = Pool::builder()
            .max_size(8)
            .connection_timeout(std::time::Duration::from_secs(10))
            .build(manager)
            .await?;

        let mut handles = Vec::new();
        for i in 1..=2 {
            let pool = pool.clone();
            let handle: JoinHandle<()> = tokio::spawn(async move {
                let mut client = pool.get().await.unwrap();
                let mut stream = client.query("SELECT TOP 1000 ID, FileNumber, Code FROM [Sample].[TestData1]", &[]).await.unwrap();
                while let Some(item) = stream.try_next().await.unwrap() {
                    match item {
                        QueryItem::Row(row) => {
                            let id: i64 = row.get(0).expect("id not found or wrong type");
                            let primary_file_row_number: i32 = row
                                .get(1)
                                .expect("FileNumber not found or wrong type");
                            let fund_code: &str =
                                row.get(2).expect("Code not found or wrong type");

                            println!(
                                "i = {}, ID = {}, FileNumber = {}, Code = {}",
                                i, id, primary_file_row_number, fund_code
                            );
                            // TODO:
                            //let id: i32 = row.try_get::<i32, _>(0)?.expect("NULL id");
                            //let name: &str = row.try_get::<&str, _>(1)?.expect("NULL name");
                            //To read just the first row of the first result quickly, you can use into_row()
                            // let row = client.query("SELECT @P1", &[&1i32]).await?
                            //                 .into_row().await?
                            //                 .unwrap();
                            // assert_eq!(Some(1i32), row.get(0))
                        }
                        QueryItem::Metadata(_meta) => {
                            //eprintln!("Error processing row: {}", e);
                            for c in _meta.columns() {
                                ;
                                let ct = c.column_type();
                                match ct {
                                    ColumnType::Int4 => println!("{}: Column type: Int4", c.name()),
                                    ColumnType::Int8 => println!("{}: Column type: Int8", c.name()),
                                    ColumnType::BigVarChar => println!("{}: Column type: BigVarChar", c.name()),
                                    _ => println!("{}: Column type: Other", c.name()),
                                }
                            }
                        }
                    }
                }
            });
            handles.push(handle);
        }

        //handle.await.expect("Task panicked");
        join_all(handles).await;

        Ok(())
    }
}
