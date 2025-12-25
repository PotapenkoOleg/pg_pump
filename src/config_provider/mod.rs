use anyhow::{Context, Result};
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    source_database: SourceDatabase,
    target_database: TargetDatabase,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SourceDatabase {
    host: String,
    port: i32,
    database: String,
    user: String,
    password: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TargetDatabase {
    host: String,
    port: i32,
    database: String,
    user: String,
    password: String,
}

pub struct ConfigProvider<'a> {
    config_file_name: &'a str,
}

impl<'a> ConfigProvider<'a> {
    pub fn new(config_file_name: &'a str) -> Self {
        Self { config_file_name }
    }

    pub async fn read_config(&self) -> Result<Config> {
        let content = tokio::fs::read_to_string(self.config_file_name)
            .await
            .with_context(|| format!("Failed to read config file: {}", self.config_file_name))?;

        let config: Config = toml::from_str(&content)?;

        Ok(config)
    }
}
