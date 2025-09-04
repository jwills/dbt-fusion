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

        // For DuckDB, allow default in-memory without setting a database-level URI.
        // Some DuckDB ADBC builds do not recognize a "uri" option at the database level.
        let _ = config; // reserved for future extension
        Ok(builder)
    }
}
