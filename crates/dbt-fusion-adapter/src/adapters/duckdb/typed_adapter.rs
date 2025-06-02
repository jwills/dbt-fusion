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
        // TODO: Implement actual DuckDB connection
        Err(AdapterError::new(
            AdapterErrorKind::Internal,
            "DuckDB connection not yet implemented",
        ))
    }

    fn self_split_statements(&self, sql: &str, _dialect: Dialect) -> Vec<String> {
        // For now, just return the SQL as a single statement
        vec![sql.to_string()]
    }

    fn execute(
        &self,
        _conn: &'_ mut dyn Connection,
        _query_ctx: &QueryCtx,
        _auto_begin: Option<bool>,
        _fetch: Option<bool>,
        _limit: Option<u32>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        // TODO: Implement actual query execution
        Err(AdapterError::new(
            AdapterErrorKind::Internal,
            "DuckDB query execution not yet implemented",
        ))
    }

    fn add_query(
        &self,
        _conn: &'_ mut dyn Connection,
        _query_ctx: &QueryCtx,
        _auto_begin: bool,
        _abridge_sql_log: bool,
    ) -> AdapterResult<()> {
        // TODO: Implement query addition
        Err(AdapterError::new(
            AdapterErrorKind::Internal,
            "DuckDB add_query not yet implemented",
        ))
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
        let sql = "SELECT 1; SELECT 2;";
        let statements = adapter.self_split_statements(sql, Dialect::DuckDB);
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0], sql);
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
}