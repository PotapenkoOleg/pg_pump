use crate::version::PRODUCT_NAME;
use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use futures::future::join_all;
use futures::TryStreamExt;
use tiberius::{AuthMethod, Config, QueryItem};
use tokio::task::JoinHandle;
use anyhow::Result;

pub async fn sql_server_test() -> Result<()> {
    let mut config = Config::new();
    config.host("");
    config.port(1433);
    config.database("");
    config.authentication(AuthMethod::sql_server("", ""));
    config.trust_cert();
    config.readonly(true);
    config.application_name(PRODUCT_NAME);

    let manager = ConnectionManager::new(config);
    let pool = Pool::builder().max_size(2).build(manager).await?;

    let mut handles = Vec::new();
    for i in 1..=10 {
        let pool = pool.clone();
        let handle: JoinHandle<()> = tokio::spawn(async move {
            let mut client = pool.get().await.unwrap();
            let mut stream = client.query("select id, PrimaryFileRowNumber, FundCode from DataMartStaging.Stage.ExtraOrdinaryLossStage_11916 WHERE Id  = @P1", &[&i]).await.unwrap();
            while let Some(item) = stream.try_next().await.unwrap() {
                match item {
                    QueryItem::Row(row) => {
                        let id: i64 = row.get(0).expect("id not found or wrong type");
                        let primary_file_row_number: i32 = row
                            .get(1)
                            .expect("PrimaryFileRowNumber not found or wrong type");
                        let fund_code: &str = row.get(2).expect("FundCode not found or wrong type");

                        println!(
                            "i= {}, id = {}, PrimaryFileRowNumber = {}, FundCode = {}",
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
                        //for c in _meta.columns() { println!("{}", c.name()); }
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
