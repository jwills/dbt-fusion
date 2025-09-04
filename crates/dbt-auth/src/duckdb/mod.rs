use crate::{AdapterConfig, Auth, AuthError};

use dbt_xdbc::{Backend, database};

#[derive(Debug, Default)]
pub struct DuckdbAuth;

impl Auth for DuckdbAuth {
    fn backend(&self) -> Backend {
        Backend::DuckDb
    }

    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        let mut builder = database::Builder::new(self.backend());

        // Default to in-memory database if not provided
        let db_path = config
            .maybe_get_str("database")?
            .unwrap_or(":memory:".to_string());

        // Construct a DuckDB URI. For in-memory, use "duckdb:"; for file, "duckdb:/path".
        let uri = if db_path == ":memory:" {
            "duckdb:".to_string()
        } else {
            // Allow absolute or relative paths. The DuckDB driver accepts duckdb:/absolute or duckdb:relative
            if db_path.starts_with("duckdb:") {
                db_path
            } else {
                format!("duckdb:{}", db_path)
            }
        };

        builder.with_parse_uri(uri)?;

        Ok(builder)
    }
}
