    
use std::sync::Arc;

use dbt_auth::AdapterConfig;
use dbt_fusion_adapter::duckdb::adapter::DuckdbAdapter;
use dbt_fusion_adapter::SqlEngine;
use dbt_fusion_adapter::TypedBaseAdapter;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_xdbc::Backend;

#[test]
fn duckdb_smoke_select_1() {
    // Only run if the DuckDB driver path or name override is provided
    if std::env::var("DUCKDB_DRIVER_PATH").is_err() && std::env::var("DUCKDB_DRIVER_NAME").is_err() {
        eprintln!("skipping duckdb_smoke_select_1: DUCKDB_DRIVER_PATH/NAME not set");
        return;
    }

        // Minimal in-memory DuckDB configuration
        let cfg = dbt_serde_yaml::Mapping::new();
        let adapter_cfg = AdapterConfig::new(cfg);

    // Build engine using DuckDB auth
    let auth = dbt_auth::auth_for_backend(Backend::DuckDb);
    let token = dbt_common::cancellation::CancellationToken::never_cancels();
    let engine = SqlEngine::new(Arc::from(auth), adapter_cfg, token);

    // Create typed adapter
    let quoting = ResolvedQuoting { database: false, schema: false, identifier: false };
    let adapter = DuckdbAdapter::new(engine.clone(), quoting, None);

    // Run a simple query
    let mut conn = adapter.new_connection(None).expect("connection");
    let ctx = dbt_xdbc::QueryCtx::new("duckdb".to_string()).with_sql("select 1 as x");
    let (_resp, table) = adapter.query(&mut *conn, &ctx, None).expect("query ok");

    assert_eq!(table.num_rows(), 1);
}
