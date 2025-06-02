use crate::adapters::base_adapter::{AdapterType, AdapterTyping};
use crate::adapters::duckdb::relation::DuckDBRelationType;
use crate::adapters::errors::{AdapterError, AdapterErrorKind, AdapterResult};
use crate::adapters::metadata::MetadataAdapter;
use crate::adapters::response::AdapterResponse;
use crate::adapters::sql_engine::SqlEngine;
use crate::adapters::typed_adapter::TypedBaseAdapter;

use arrow::array::RecordBatch;
use arrow_schema::{DataType, Schema};
use dbt_agate::AgateTable;
use dbt_frontend_schemas::dialect::Dialect;
use dbt_schemas::schemas::columns::base::BaseColumn;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::relations::base::{BaseRelation, ComponentName};
use dbt_xdbc::{Connection, QueryCtx};
use minijinja::{State, Value};

use std::fmt;
use std::sync::Arc;

/// DuckDB TypedBaseAdapter implementation
#[derive(Clone)]
pub struct DuckDBTypedAdapter {
    engine: Option<Arc<SqlEngine>>,
}

impl DuckDBTypedAdapter {
    /// Create a new DuckDB adapter
    pub fn new() -> Self {
        Self { engine: None }
    }

    /// Create a new DuckDB adapter with an SQL engine
    pub fn with_engine(engine: Arc<SqlEngine>) -> Self {
        Self {
            engine: Some(engine),
        }
    }
}

impl Default for DuckDBTypedAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for DuckDBTypedAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckDBTypedAdapter")
            .field("engine", &self.engine.is_some())
            .finish()
    }
}

impl TypedBaseAdapter for DuckDBTypedAdapter {
    fn new_connection(&self) -> AdapterResult<Box<dyn Connection>> {
        match &self.engine {
            Some(engine) => engine.new_connection(),
            None => Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "No SQL engine configured for DuckDB adapter. Use DuckDBTypedAdapter::with_engine() to provide an engine.",
            )),
        }
    }

    fn self_split_statements(&self, sql: &str, _dialect: Dialect) -> Vec<String> {
        // Simple SQL statement splitting for DuckDB
        // This is a basic implementation - a more sophisticated parser would handle
        // string literals, comments, and other SQL constructs properly
        sql.split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }

    fn execute(
        &self,
        conn: &'_ mut dyn Connection,
        query_ctx: &QueryCtx,
        auto_begin: Option<bool>,
        fetch: Option<bool>,
        limit: Option<u32>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        // Get the SQL engine, required for query execution
        let engine = self.engine.as_ref().ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                "No SQL engine configured for DuckDB adapter. Use DuckDBTypedAdapter::with_engine() to provide an engine.",
            )
        })?;

        // Use the default execute_inner implementation
        self.execute_inner(
            Dialect::DuckDB,
            engine.clone(),
            conn,
            query_ctx,
            auto_begin,
            fetch,
            limit,
        )
    }

    fn add_query(
        &self,
        conn: &'_ mut dyn Connection,
        query_ctx: &QueryCtx,
        _auto_begin: bool,
        _abridge_sql_log: bool,
    ) -> AdapterResult<()> {
        // Get the SQL engine, required for query execution
        let engine = self.engine.as_ref().ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                "No SQL engine configured for DuckDB adapter. Use DuckDBTypedAdapter::with_engine() to provide an engine.",
            )
        })?;

        // For DuckDB, we'll execute the query immediately since we don't have
        // sophisticated transaction management yet. In a real implementation,
        // this might batch queries for better performance.
        let sql = query_ctx.sql().ok_or_else(|| {
            AdapterError::new(AdapterErrorKind::Internal, "Missing query in the context")
        })?;

        // Split statements and execute each one
        let statements = self.self_split_statements(&sql, Dialect::DuckDB);
        for statement in statements {
            // Execute each statement, ignoring results for add_query
            crate::adapters::sql_engine::execute_query_with_retry(
                engine.clone(),
                conn,
                &query_ctx.with_sql(statement),
                1,
            )?;
        }

        Ok(())
    }

    fn quote(&self, identifier: &str) -> String {
        // DuckDB uses double quotes like PostgreSQL
        format!("\"{}\"", identifier.replace("\"", "\"\""))
    }

    fn list_schemas(&self, _result: Arc<RecordBatch>) -> Vec<String> {
        // TODO: Implement schema listing from RecordBatch
        vec![]
    }

    fn get_relation(
        &self,
        _query_ctx: &QueryCtx,
        _conn: &'_ mut dyn Connection,
        _database: &str,
        _schema: &str,
        _identifier: &str,
        _needs_information: Option<bool>,
    ) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
        // TODO: Implement relation retrieval
        Ok(None)
    }

    fn drop_relation(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value> {
        // TODO: Implement relation dropping
        Err(AdapterError::new(
            AdapterErrorKind::Internal,
            "DuckDB drop_relation not yet implemented",
        ))
    }

    fn check_schema_exists_macro(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> AdapterResult<(String, String)> {
        // Return default macro package and name
        Ok(("dbt".to_string(), "check_schema_exists".to_string()))
    }

    fn rename_relation(
        &self,
        _conn: &'_ mut dyn Connection,
        _from_relation: Arc<dyn BaseRelation>,
        _to_relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<()> {
        // TODO: Implement relation renaming
        Err(AdapterError::new(
            AdapterErrorKind::Internal,
            "DuckDB rename_relation not yet implemented",
        ))
    }

    fn get_missing_columns(
        &self,
        _state: &State,
        _source_relation: Arc<dyn BaseRelation>,
        _target_relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value> {
        // TODO: Implement missing column detection
        Ok(Value::from(Vec::<Value>::new()))
    }

    fn get_columns_in_relation(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Vec<Box<dyn BaseColumn>>> {
        // TODO: Implement column retrieval
        Ok(vec![])
    }

    fn arrow_schema_to_dbt_columns(&self, _schema: Arc<Schema>) -> AdapterResult<Vec<Value>> {
        // TODO: Implement Arrow schema to dbt columns conversion
        Ok(vec![])
    }

    fn truncate_relation(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value> {
        // TODO: Implement relation truncation
        Err(AdapterError::new(
            AdapterErrorKind::Internal,
            "DuckDB truncate_relation not yet implemented",
        ))
    }

    fn quote_as_configured(&self, identifier: &str, quote_key: &ComponentName) -> String {
        // DuckDB follows PostgreSQL-like behavior - quote when needed
        match quote_key {
            ComponentName::Database | ComponentName::Schema | ComponentName::Identifier => {
                self.quote(identifier)
            }
        }
    }

    fn get_resolved_quoting(&self) -> ResolvedQuoting {
        // DuckDB default quoting behavior (similar to PostgreSQL)
        ResolvedQuoting {
            database: false,
            schema: false,
            identifier: false,
        }
    }

    fn convert_type_inner(&self, _data_type: &DataType) -> AdapterResult<String> {
        // TODO: Implement Arrow DataType to DuckDB SQL type conversion
        Ok("TEXT".to_string()) // Default to TEXT for now
    }

    fn get_column_schema_from_query(
        &self,
        _conn: &mut dyn Connection,
        _query_ctx: &QueryCtx,
    ) -> AdapterResult<Vec<Box<dyn BaseColumn>>> {
        // TODO: Implement column schema retrieval from query
        Ok(vec![])
    }
}

impl AdapterTyping for DuckDBTypedAdapter {
    fn adapter_type(&self) -> AdapterType {
        AdapterType::DuckDB
    }

    fn as_metadata_adapter(&self) -> Option<&dyn MetadataAdapter> {
        None // DuckDB doesn't implement metadata adapter yet
    }

    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter {
        self
    }

    fn relation_type(&self) -> Option<Value> {
        Some(Value::from_object(DuckDBRelationType))
    }

    fn column_type(&self) -> Option<Value> {
        None // TODO: Implement column type
    }

    fn engine(&self) -> Option<&Arc<SqlEngine>> {
        self.engine.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_duckdb_adapter() {
        let adapter = DuckDBTypedAdapter::new();
        assert_eq!(adapter.adapter_type(), AdapterType::DuckDB);
        assert!(adapter.relation_type().is_some());
        assert!(adapter.column_type().is_none());
        assert!(adapter.engine().is_none());
    }

    #[test]
    fn test_duckdb_adapter_default() {
        let adapter = DuckDBTypedAdapter::default();
        assert_eq!(adapter.adapter_type(), AdapterType::DuckDB);
    }

    #[test]
    fn test_duckdb_quote() {
        let adapter = DuckDBTypedAdapter::new();
        assert_eq!(adapter.quote("test"), "\"test\"");
        assert_eq!(adapter.quote("test\"quote"), "\"test\"\"quote\"");
    }

    #[test]
    fn test_duckdb_split_statements() {
        let adapter = DuckDBTypedAdapter::new();
        
        // Test single statement
        let sql = "SELECT 1";
        let statements = adapter.self_split_statements(sql, Dialect::DuckDB);
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0], "SELECT 1");
        
        // Test multiple statements
        let sql = "SELECT 1; SELECT 2;";
        let statements = adapter.self_split_statements(sql, Dialect::DuckDB);
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0], "SELECT 1");
        assert_eq!(statements[1], "SELECT 2");
        
        // Test with whitespace
        let sql = "  SELECT 1 ;   SELECT 2  ; ";
        let statements = adapter.self_split_statements(sql, Dialect::DuckDB);
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0], "SELECT 1");
        assert_eq!(statements[1], "SELECT 2");
        
        // Test empty statements
        let sql = ";;SELECT 1;;";
        let statements = adapter.self_split_statements(sql, Dialect::DuckDB);
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0], "SELECT 1");
    }

    #[test]
    fn test_duckdb_quote_as_configured() {
        let adapter = DuckDBTypedAdapter::new();
        assert_eq!(
            adapter.quote_as_configured("test", &ComponentName::Identifier),
            "\"test\""
        );
    }

    #[test]
    fn test_duckdb_resolved_quoting() {
        let adapter = DuckDBTypedAdapter::new();
        let quoting = adapter.get_resolved_quoting();
        assert_eq!(quoting.database, false);
        assert_eq!(quoting.schema, false);
        assert_eq!(quoting.identifier, false);
    }

    #[test]
    fn test_duckdb_convert_type_inner() {
        let adapter = DuckDBTypedAdapter::new();
        let result = adapter.convert_type_inner(&DataType::Utf8).unwrap();
        assert_eq!(result, "TEXT");
    }

    #[test]
    fn test_bridge_adapter_integration() {
        use crate::adapters::bridge_adapter::BridgeAdapter;
        use std::sync::Arc;

        // Create DuckDB typed adapter
        let typed_adapter = DuckDBTypedAdapter::new();
        assert_eq!(typed_adapter.adapter_type(), AdapterType::DuckDB);

        // Wrap it in BridgeAdapter
        let bridge_adapter = BridgeAdapter::new(Arc::new(typed_adapter), None);
        
        // Verify BridgeAdapter correctly reports DuckDB adapter type
        assert_eq!(bridge_adapter.adapter_type(), AdapterType::DuckDB);
        
        // Verify relation type is available
        assert!(bridge_adapter.relation_type().is_some());
        
        // Verify engine is None (no engine set)
        assert!(bridge_adapter.engine().is_none());
        
        // Verify Display formatting
        assert_eq!(bridge_adapter.to_string(), "Adapter(duckdb)");
    }

    #[test]
    fn test_relation_type_factory() {
        use crate::adapters::bridge_adapter::relation_type_from_adapter_type;
        
        // Test that DuckDB adapter type returns the correct relation type
        let relation_type = relation_type_from_adapter_type(AdapterType::DuckDB);
        assert!(relation_type.is_some());
        
        // Verify it can be downcast to DuckDBRelationType
        let value = relation_type.unwrap();
        let duckdb_relation_type = value.downcast_object_ref::<DuckDBRelationType>();
        assert!(duckdb_relation_type.is_some());
    }

    #[test]
    fn test_duckdb_connection_creation() {
        use crate::adapters::config::AdapterConfig;
        use crate::adapters::duckdb::auth::DuckDBAuth;
        use crate::adapters::auth::Auth;
        use crate::adapters::sql_engine::SqlEngine;
        use std::collections::HashMap;

        // Create DuckDB auth for in-memory database
        let auth = DuckDBAuth::memory();
        assert_eq!(auth.backend(), dbt_xdbc::Backend::DuckDB);

        // Create adapter config
        let mut db_config = HashMap::new();
        db_config.insert("path".to_string(), serde_json::Value::String(":memory:".to_string()));
        let config = AdapterConfig::new(db_config);

        // Create SQL engine with auth and config
        let engine = SqlEngine::new(Arc::new(auth), config);

        // Create DuckDB adapter with engine
        let adapter = DuckDBTypedAdapter::with_engine(engine);
        assert_eq!(adapter.adapter_type(), AdapterType::DuckDB);
        assert!(adapter.engine().is_some());

        // Test that new_connection doesn't error (actual connection may fail without DuckDB driver)
        // But the method should delegate properly to the engine
        let connection_result = adapter.new_connection();
        // We expect either success or a driver-level error, not a configuration error
        match connection_result {
            Ok(_) => {
                // Connection succeeded - great!
            }
            Err(e) => {
                // Should be a driver/XDBC error, not a configuration error
                assert!(matches!(e.kind(), AdapterErrorKind::Xdbc(_)));
            }
        }
    }

    #[test]
    fn test_duckdb_connection_without_engine() {
        // Test adapter without engine configuration
        let adapter = DuckDBTypedAdapter::new();
        
        // Should return configuration error
        let connection_result = adapter.new_connection();
        assert!(connection_result.is_err());
        
        let error = connection_result.unwrap_err();
        assert!(matches!(error.kind(), AdapterErrorKind::Configuration));
        assert!(error.to_string().contains("No SQL engine configured"));
    }

    #[test]
    fn test_duckdb_execute_without_engine() {
        use crate::adapters::config::AdapterConfig;
        use crate::adapters::duckdb::auth::DuckDBAuth;
        use crate::adapters::sql_engine::SqlEngine;
        use std::collections::HashMap;

        // Test execute without engine configuration
        let adapter = DuckDBTypedAdapter::new();
        
        // Create an engine for testing (this will be used to create a connection, but the execute method will error first)
        let auth = DuckDBAuth::memory();
        let db_config = HashMap::new();
        let config = AdapterConfig::new(db_config);
        let engine = SqlEngine::new(Arc::new(auth), config);
        
        // Get a connection that we can use for testing (though it won't actually be used)
        let adapter_with_engine = DuckDBTypedAdapter::with_engine(engine);
        let connection_result = adapter_with_engine.new_connection();
        
        // Skip this test if we can't create a connection (DuckDB driver not available)
        if connection_result.is_err() {
            return;
        }
        let mut conn = connection_result.unwrap();
        
        // Create query context
        let query_ctx = QueryCtx::new("duckdb").with_sql("SELECT 1");
        
        // Should return configuration error from the adapter without engine
        let execute_result = adapter.execute(&mut *conn, &query_ctx, None, None, None);
        assert!(execute_result.is_err());
        
        let error = execute_result.unwrap_err();
        assert!(matches!(error.kind(), AdapterErrorKind::Configuration));
        assert!(error.to_string().contains("No SQL engine configured"));
    }

    #[test]
    fn test_duckdb_add_query_without_engine() {
        use crate::adapters::config::AdapterConfig;
        use crate::adapters::duckdb::auth::DuckDBAuth;
        use crate::adapters::sql_engine::SqlEngine;
        use std::collections::HashMap;

        // Test add_query without engine configuration
        let adapter = DuckDBTypedAdapter::new();
        
        // Create an engine for testing (this will be used to create a connection, but the add_query method will error first)
        let auth = DuckDBAuth::memory();
        let db_config = HashMap::new();
        let config = AdapterConfig::new(db_config);
        let engine = SqlEngine::new(Arc::new(auth), config);
        
        // Get a connection that we can use for testing (though it won't actually be used)
        let adapter_with_engine = DuckDBTypedAdapter::with_engine(engine);
        let connection_result = adapter_with_engine.new_connection();
        
        // Skip this test if we can't create a connection (DuckDB driver not available)
        if connection_result.is_err() {
            return;
        }
        let mut conn = connection_result.unwrap();
        
        // Create query context
        let query_ctx = QueryCtx::new("duckdb").with_sql("SELECT 1");
        
        // Should return configuration error from the adapter without engine
        let add_query_result = adapter.add_query(&mut *conn, &query_ctx, true, false);
        assert!(add_query_result.is_err());
        
        let error = add_query_result.unwrap_err();
        assert!(matches!(error.kind(), AdapterErrorKind::Configuration));
        assert!(error.to_string().contains("No SQL engine configured"));
    }

    #[test]
    fn test_duckdb_sql_execution_integration() {
        use crate::adapters::config::AdapterConfig;
        use crate::adapters::duckdb::auth::DuckDBAuth;
        use crate::adapters::sql_engine::SqlEngine;
        use std::collections::HashMap;

        // Create DuckDB adapter with engine
        let auth = DuckDBAuth::memory();
        let db_config = HashMap::new();
        let config = AdapterConfig::new(db_config);
        let engine = SqlEngine::new(Arc::new(auth), config);
        let adapter = DuckDBTypedAdapter::with_engine(engine);

        // Try to create a connection
        let connection_result = adapter.new_connection();
        
        // Skip this test if we can't create a connection (DuckDB driver not available)
        if connection_result.is_err() {
            eprintln!("Skipping DuckDB SQL execution test - driver not available: {:?}", connection_result);
            return;
        }
        
        let mut conn = connection_result.unwrap();

        // Test simple SELECT query
        let query_ctx = QueryCtx::new("duckdb").with_sql("SELECT 1 as test_value");
        let execute_result = adapter.execute(&mut *conn, &query_ctx, None, Some(true), None);
        
        match execute_result {
            Ok((response, table)) => {
                // Execution succeeded
                println!("Execute succeeded: {:?}", response);
                println!("Table columns: {:?}", table.column_names());
            }
            Err(e) => {
                // This is expected if the DuckDB ADBC driver isn't properly installed
                // The important thing is that we get an XDBC error, not a configuration error
                println!("Execute failed (expected if DuckDB driver not available): {:?}", e);
                assert!(matches!(e.kind(), AdapterErrorKind::Xdbc(_)));
            }
        }

        // Test add_query functionality
        let add_query_ctx = QueryCtx::new("duckdb").with_sql("CREATE TABLE IF NOT EXISTS test (id INTEGER)");
        let add_result = adapter.add_query(&mut *conn, &add_query_ctx, false, false);
        
        match add_result {
            Ok(()) => {
                println!("Add query succeeded");
            }
            Err(e) => {
                // This is expected if the DuckDB ADBC driver isn't properly installed
                println!("Add query failed (expected if DuckDB driver not available): {:?}", e);
                assert!(matches!(e.kind(), AdapterErrorKind::Xdbc(_)));
            }
        }
    }

    #[test]
    fn test_duckdb_statement_splitting() {
        let adapter = DuckDBTypedAdapter::new();
        
        // Test single statement
        let single = adapter.self_split_statements("SELECT 1", Dialect::DuckDB);
        assert_eq!(single, vec!["SELECT 1"]);
        
        // Test multiple statements
        let multiple = adapter.self_split_statements("SELECT 1; SELECT 2; CREATE TABLE test (id INTEGER)", Dialect::DuckDB);
        assert_eq!(multiple, vec!["SELECT 1", "SELECT 2", "CREATE TABLE test (id INTEGER)"]);
        
        // Test statements with whitespace
        let whitespace = adapter.self_split_statements("  SELECT 1  ;  \n  SELECT 2  ;  ", Dialect::DuckDB);
        assert_eq!(whitespace, vec!["SELECT 1", "SELECT 2"]);
        
        // Test empty and semicolon-only input
        let empty = adapter.self_split_statements("", Dialect::DuckDB);
        assert_eq!(empty, Vec::<String>::new());
        
        let semicolons = adapter.self_split_statements(";;;", Dialect::DuckDB);
        assert_eq!(semicolons, Vec::<String>::new());
    }
}