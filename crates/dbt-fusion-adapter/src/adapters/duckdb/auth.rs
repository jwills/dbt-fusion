use crate::adapters::auth::Auth;
use crate::adapters::config::AdapterConfig;
use crate::adapters::errors::AdapterResult;

use dbt_xdbc::{database, Backend};
use std::fmt;

/// DuckDB authentication implementation
#[derive(Clone, Debug)]
pub struct DuckDBAuth {
    /// Database path - can be a file path or `:memory:` for in-memory database
    pub path: Option<String>,
    /// Number of threads for DuckDB to use
    pub threads: Option<i32>,
    /// Memory limit for DuckDB
    pub memory_limit: Option<String>,
    /// Temporary directory for DuckDB operations
    pub temp_directory: Option<String>,
}

impl DuckDBAuth {
    /// Create a new DuckDB auth configuration
    pub fn new() -> Self {
        Self {
            path: None,
            threads: None,
            memory_limit: None,
            temp_directory: None,
        }
    }

    /// Create a new DuckDB auth configuration with a database path
    pub fn with_path(path: impl Into<String>) -> Self {
        Self {
            path: Some(path.into()),
            threads: None,
            memory_limit: None,
            temp_directory: None,
        }
    }

    /// Create a new DuckDB auth configuration for in-memory database
    pub fn memory() -> Self {
        Self::with_path(":memory:")
    }

    /// Set number of threads
    pub fn with_threads(mut self, threads: i32) -> Self {
        self.threads = Some(threads);
        self
    }

    /// Set memory limit
    pub fn with_memory_limit(mut self, memory_limit: impl Into<String>) -> Self {
        self.memory_limit = Some(memory_limit.into());
        self
    }

    /// Set temporary directory
    pub fn with_temp_directory(mut self, temp_directory: impl Into<String>) -> Self {
        self.temp_directory = Some(temp_directory.into());
        self
    }

}

impl Default for DuckDBAuth {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for DuckDBAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DuckDBAuth(path={:?})", self.path)
    }
}

impl Auth for DuckDBAuth {
    fn backend(&self) -> Backend {
        Backend::DuckDB
    }

    fn configure(&self, config: &AdapterConfig) -> AdapterResult<database::Builder> {
        let mut builder = database::Builder::new(Backend::DuckDB);

        // Configure database path - check config first, then use auth setting
        // DuckDB ADBC uses simple "path" option name, not hierarchical naming
        let path = if let Some(config_path) = config.maybe_get_str("path")? {
            config_path
        } else if let Some(auth_path) = &self.path {
            auth_path.clone()
        } else {
            // Default to in-memory database if no path specified
            ":memory:".to_string()
        };

        // Set the database path using DuckDB's simple option naming
        builder.with_named_option("path", path)?;

        // Configure thread count if specified
        if let Some(config_threads) = config.maybe_get_str("threads")? {
            builder.with_named_option("threads", config_threads)?;
        } else if let Some(auth_threads) = self.threads {
            builder.with_named_option("threads", auth_threads.to_string())?;
        }

        // Configure memory limit if specified
        if let Some(memory_limit) = config.maybe_get_str("memory_limit")?.or(self.memory_limit.as_ref().cloned()) {
            builder.with_named_option("memory_limit", memory_limit)?;
        }

        // Configure temp directory if specified
        if let Some(temp_directory) = config.maybe_get_str("temp_directory")?.or(self.temp_directory.as_ref().cloned()) {
            builder.with_named_option("temp_directory", temp_directory)?;
        }

        // Note: Extensions are handled at the adapter level via SQL commands after connection
        // We don't configure them here in the ADBC builder

        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_config(values: HashMap<String, String>) -> AdapterConfig {
        let mut db_config = HashMap::new();
        for (k, v) in values {
            db_config.insert(k, serde_json::Value::String(v));
        }
        AdapterConfig::new(db_config)
    }

    #[test]
    fn test_duckdb_auth_new() {
        let auth = DuckDBAuth::new();
        assert_eq!(auth.backend(), Backend::DuckDB);
        assert_eq!(auth.path, None);
        assert_eq!(auth.threads, None);
    }

    #[test]
    fn test_duckdb_auth_with_path() {
        let auth = DuckDBAuth::with_path("/tmp/test.db");
        assert_eq!(auth.path, Some("/tmp/test.db".to_string()));
        assert_eq!(auth.backend(), Backend::DuckDB);
    }

    #[test]
    fn test_duckdb_auth_memory() {
        let auth = DuckDBAuth::memory();
        assert_eq!(auth.path, Some(":memory:".to_string()));
        assert_eq!(auth.backend(), Backend::DuckDB);
    }

    #[test]
    fn test_duckdb_auth_configure_default() {
        let auth = DuckDBAuth::new();
        let config = create_config(HashMap::new());
        let _builder = auth.configure(&config).unwrap();
        
        // Should use default in-memory database
        // We can't easily test the internal state of builder, but we can test that it doesn't error
        assert_eq!(auth.backend(), Backend::DuckDB);
    }

    #[test]
    fn test_duckdb_auth_configure_with_path() {
        let auth = DuckDBAuth::with_path("/tmp/test.db");
        let config = create_config(HashMap::new());
        let _builder = auth.configure(&config).unwrap();
        // Configuration should succeed
    }

    #[test]
    fn test_duckdb_auth_configure_from_config() {
        let auth = DuckDBAuth::new();
        let mut config_values = HashMap::new();
        config_values.insert("path".to_string(), "/config/test.db".to_string());
        config_values.insert("threads".to_string(), "4".to_string());
        
        let config = create_config(config_values);
        let _builder = auth.configure(&config).unwrap();
        // Configuration should succeed with config values taking precedence
    }

    #[test]
    fn test_duckdb_auth_builder_methods() {
        let auth = DuckDBAuth::new()
            .with_threads(8)
            .with_memory_limit("1GB")
            .with_temp_directory("/tmp");

        assert_eq!(auth.threads, Some(8));
        assert_eq!(auth.memory_limit, Some("1GB".to_string()));
        assert_eq!(auth.temp_directory, Some("/tmp".to_string()));
    }

    #[test]
    fn test_duckdb_auth_display() {
        let auth = DuckDBAuth::with_path("/test.db");
        let display = format!("{}", auth);
        assert!(display.contains("DuckDBAuth"));
        assert!(display.contains("/test.db"));
    }

}