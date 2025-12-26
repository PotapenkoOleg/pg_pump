use anyhow::Result;
use rand::Rng;
use rand::distr::Alphanumeric;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

pub async fn seed_test_data() -> Result<()> {
    let mut config = Config::new();
    config.host("localhost");
    config.port(1433);
    config.database("developer");
    config.authentication(AuthMethod::sql_server("sa", "wOf-odESTO2r"));
    config.trust_cert();
    config.encryption(EncryptionLevel::NotSupported);

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut client = Client::connect(config, tcp.compat_write()).await?;

    println!("Connected to SQL Server. Starting insertion of 10,000 records...");

    let mut rng = rand::rng();

    for i in 1..=100_000i64 {
        let file_number: i32 = rng.random_range(1000..999999);
        let code: String = std::iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .map(char::from)
            .take(10)
            .collect();

        client
            .execute(
                "INSERT INTO [Sample].[TestData1] (ID, FileNumber, Code) VALUES (@P1, @P2, @P3)",
                &[&i, &file_number, &format!("{}", code)],
            )
            .await?;

        if i % 1000 == 0 {
            println!("Inserted {} records...", i);
        }
    }

    println!("Finished inserting 100,000 records.");
    Ok(())
}
