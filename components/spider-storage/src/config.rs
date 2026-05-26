use secrecy::SecretString;
use serde::{Deserialize, Serialize};

/// TLS mode for MariaDB/MySQL database connections.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseSslMode {
    Disabled,
    #[default]
    Preferred,
    Required,
    VerifyCa,
    VerifyIdentity,
}

/// Configuration parameters for connecting to the database.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub username: String,
    #[serde(skip_serializing)]
    pub password: SecretString,
    pub max_connections: u32,
    #[serde(default)]
    pub ssl_mode: DatabaseSslMode,
}
