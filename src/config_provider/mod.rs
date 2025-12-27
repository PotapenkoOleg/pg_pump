use anyhow::{Context, Result};
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    source_database: SourceDatabase,
    target_database: TargetDatabase,
}

impl Config {
    pub fn get_source_database_as_ref(&self) -> &SourceDatabase {
        &self.source_database
    }

    pub fn get_target_database_as_ref(&self) -> &TargetDatabase {
        &self.target_database
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SourceDatabase {
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
}

impl SourceDatabase {
    pub fn get_host_as_ref(&self) -> &String {
        &self.host
    }

    pub fn get_port_as_ref(&self) -> &u16 {
        &self.port
    }

    pub fn get_database_as_ref(&self) -> &String {
        &self.database
    }

    pub fn get_user_as_ref(&self) -> &String {
        &self.user
    }

    pub fn get_password_as_ref(&self) -> &String {
        &self.password
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TargetDatabase {
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
}

impl TargetDatabase {
    pub fn get_host_as_ref(&self) -> &String {
        &self.host
    }

    pub fn get_port_as_ref(&self) -> &u16 {
        &self.port
    }

    pub fn get_database_as_ref(&self) -> &String {
        &self.database
    }

    pub fn get_user_as_ref(&self) -> &String {
        &self.user
    }

    pub fn get_password_as_ref(&self) -> &String {
        &self.password
    }
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
