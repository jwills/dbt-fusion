use crate::AdapterType;
use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult};
use crate::relation_object::create_relation;
use crate::sql_engine::SqlEngine;
use crate::typed_adapter::TypedBaseAdapter;
use crate::AdapterTyping;

use arrow::array::Array;
use arrow_schema::DataType;
use dbt_common::adapter::SchemaRegistry;
use dbt_common::cancellation::CancellationToken;
use dbt_frontend_common::dialect::Dialect;
use dbt_schemas::schemas::columns::base::{BaseColumn, StdColumn};
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::relations::base::BaseRelation;
use dbt_xdbc::{Connection, QueryCtx};
use minijinja::Value;

use std::fmt;
use std::sync::Arc;

#[derive(Clone)]
pub struct DuckdbAdapter {
    engine: Arc<SqlEngine>,
    quoting: ResolvedQuoting,
    #[allow(dead_code)]
    db: Option<Arc<dyn SchemaRegistry>>,
}

impl DuckdbAdapter {
    pub fn new(engine: Arc<SqlEngine>, quoting: ResolvedQuoting, db: Option<Arc<dyn SchemaRegistry>>) -> Self {
        Self { engine, quoting, db }
    }

    fn dialect(&self) -> Dialect {
        Dialect::from(AdapterType::Duckdb)
    }

    fn quote_ident(&self, ident: &str) -> String {
        let escaped = ident.replace('"', "\"\"");
        format!("\"{}\"", escaped)
    }
}

impl fmt::Debug for DuckdbAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DuckdbAdapter")
    }
}

impl AdapterTyping for DuckdbAdapter {
    fn adapter_type(&self) -> AdapterType { AdapterType::Duckdb }

    fn as_metadata_adapter(&self) -> Option<&dyn crate::metadata::MetadataAdapter> { None }

    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter { self }

    fn column_type(&self) -> Option<Value> {
        Some(Value::from_object(dbt_schemas::schemas::columns::base::StdColumnType))
    }

    fn engine(&self) -> Option<&Arc<SqlEngine>> { Some(&self.engine) }

    fn quoting(&self) -> ResolvedQuoting { self.quoting }

    fn cancellation_token(&self) -> CancellationToken { self.engine.cancellation_token() }
}

impl TypedBaseAdapter for DuckdbAdapter {
    fn new_connection(&self, node_id: Option<String>) -> AdapterResult<Box<dyn Connection>> {
        self.engine.new_connection(node_id).map_err(Into::into)
    }

    fn self_split_statements(&self, sql: &str, _dialect: Dialect) -> Vec<String> {
        let mut parts = Vec::new();
        let mut start = 0usize;
        let mut in_single = false;
        let mut in_double = false;
        let bytes = sql.as_bytes();
        let mut i = 0usize;
        while i < bytes.len() {
            let c = bytes[i] as char;
            match c {
                '\'' if !in_double => in_single = !in_single,
                '"' if !in_single => in_double = !in_double,
                ';' if !in_single && !in_double => {
                    parts.push(sql[start..i].to_string());
                    start = i + 1;
                }
                _ => {}
            }
            i += 1;
        }
        if start <= sql.len() {
            parts.push(sql[start..].to_string());
        }
        parts
    }

    fn execute(
        &self,
        conn: &'_ mut dyn Connection,
        query_ctx: &QueryCtx,
        auto_begin: bool,
        fetch: bool,
        limit: Option<i64>,
    ) -> AdapterResult<(crate::response::AdapterResponse, dbt_agate::AgateTable)> {
        let dialect = self.dialect();
        self.execute_inner(dialect, self.engine.clone(), conn, query_ctx, auto_begin, fetch, limit)
    }

    fn add_query(
        &self,
        conn: &'_ mut dyn Connection,
        query_ctx: &QueryCtx,
        auto_begin: bool,
        _bindings: Option<&Value>,
        _abridge_sql_log: bool,
    ) -> AdapterResult<()> {
        let _ = self.exec_stmt(conn, query_ctx, auto_begin)?;
        Ok(())
    }

    fn quote(&self, identifier: &str) -> String {
        self.quote_ident(identifier)
    }

    fn list_schemas(&self, result: Arc<arrow::array::RecordBatch>) -> AdapterResult<Vec<String>> {
        if result.num_columns() == 0 {
            return Ok(vec![]);
        }
        let col = result.column(0);
        let arr = col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| AdapterError::new(AdapterErrorKind::Internal, "unexpected list_schemas result type"))?;
        let mut out = Vec::with_capacity(arr.len());
        for i in 0..arr.len() {
            if arr.is_null(i) { continue; }
            out.push(arr.value(i).to_string());
        }
        Ok(out)
    }

    fn get_relation(
        &self,
        query_ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        _database: &str,
        schema: &str,
        identifier: &str,
    ) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
        let esc = |s: &str| s.replace('\'', "''");
        let sql = format!(
            "select 1 from information_schema.tables where table_schema = '{}' and table_name = '{}' limit 1",
            esc(schema),
            esc(identifier)
        );
        let batch = self.engine.execute(conn, &query_ctx.with_sql(sql))?;
        if batch.num_rows() > 0 {
            let relation = create_relation(
                self.adapter_type().to_string(),
                "main".to_string(),
                schema.to_string(),
                Some(identifier.to_string()),
                None,
                self.get_resolved_quoting(),
            )?;
            Ok(Some(relation))
        } else {
            Ok(None)
        }
    }

    fn get_columns_in_relation(
        &self,
        _state: &minijinja::State,
        relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Vec<Box<dyn BaseColumn>>> {
        let fqn = relation.render_self_as_str();
        let sql = format!("select * from {} limit 0", fqn);
        let mut conn = self.new_connection(None)?;
        let ctx = QueryCtx::new(self.adapter_type().to_string()).with_sql(sql);
        let batch = self.engine.execute(&mut *conn, &ctx)?;
        let schema = batch.schema();
        let mut out: Vec<Box<dyn BaseColumn>> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let db_type = self.convert_type_inner(field.data_type())?;
            let col = StdColumn { name: field.name().clone(), dtype: db_type, char_size: None, numeric_precision: None, numeric_scale: None };
            out.push(Box::new(col));
        }
        Ok(out)
    }

    fn arrow_schema_to_dbt_columns(
        &self,
        schema: Arc<arrow_schema::Schema>,
    ) -> AdapterResult<Vec<Arc<dyn BaseColumn>>> {
        let mut out: Vec<Arc<dyn BaseColumn>> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let name = field.name().clone();
            let dtype = field.data_type();
            let db_type = self.convert_type_inner(dtype)?;
            let col = StdColumn { name, dtype: db_type, char_size: None, numeric_precision: None, numeric_scale: None };
            out.push(Arc::new(col));
        }
        Ok(out)
    }

    fn get_resolved_quoting(&self) -> ResolvedQuoting { self.quoting }

    fn convert_type_inner(&self, data_type: &DataType) -> AdapterResult<String> {
        let t = match data_type {
            DataType::Null => "INTEGER",
            DataType::Boolean => "BOOLEAN",
            DataType::Int8 | DataType::Int16 | DataType::Int32 => "INTEGER",
            DataType::Int64 => "BIGINT",
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => "INTEGER",
            DataType::UInt64 => "UBIGINT",
            DataType::Float16 | DataType::Float32 => "REAL",
            DataType::Float64 => "DOUBLE",
            DataType::Utf8 | DataType::LargeUtf8 => "TEXT",
            DataType::Binary | DataType::LargeBinary => "BLOB",
            DataType::Timestamp(_, _) => "TIMESTAMP",
            DataType::Date32 | DataType::Date64 => "DATE",
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "DECIMAL",
            _ => "TEXT",
        };
        Ok(t.to_string())
    }

    fn get_column_schema_from_query(
        &self,
        conn: &mut dyn Connection,
        query_ctx: &QueryCtx,
    ) -> AdapterResult<Vec<Box<dyn BaseColumn>>> {
        let sql = query_ctx.sql().ok_or_else(|| {
            AdapterError::new(AdapterErrorKind::Internal, "Missing SQL for get_column_schema_from_query")
        })?;
        let wrapped = format!("select * from ({}) as t limit 0", sql);
        let batch = self.engine.execute(conn, &query_ctx.with_sql(wrapped))?;
        let schema = batch.schema();
        let mut out: Vec<Box<dyn BaseColumn>> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let db_type = self.convert_type_inner(field.data_type())?;
            let col = StdColumn { name: field.name().clone(), dtype: db_type, char_size: None, numeric_precision: None, numeric_scale: None };
            out.push(Box::new(col));
        }
        Ok(out)
    }
}

