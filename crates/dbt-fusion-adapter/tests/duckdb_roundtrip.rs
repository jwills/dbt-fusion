    
use std::sync::Arc;

use dbt_auth::AdapterConfig;
use dbt_fusion_adapter::duckdb::adapter::DuckdbAdapter;
use dbt_fusion_adapter::AdapterTyping;
use dbt_fusion_adapter::TypedBaseAdapter;
use dbt_fusion_adapter::relation_object::create_relation;
use dbt_fusion_adapter::SqlEngine;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_xdbc::Backend;

#[test]
fn duckdb_roundtrip_relation_and_columns() {
    if std::env::var("DUCKDB_DRIVER_PATH").is_err() && std::env::var("DUCKDB_DRIVER_NAME").is_err() {
        eprintln!("skipping roundtrip: DUCKDB_DRIVER_PATH/NAME not set");
        return;
    }

        let adapter_cfg = AdapterConfig::new(dbt_serde_yaml::Mapping::new());
    let auth = dbt_auth::auth_for_backend(Backend::DuckDb);
    let token = dbt_common::cancellation::CancellationToken::never_cancels();
    let engine = SqlEngine::new(Arc::from(auth), adapter_cfg, token);
    let quoting = ResolvedQuoting { database: false, schema: false, identifier: false };
    let adapter = DuckdbAdapter::new(engine.clone(), quoting, None);

    let mut conn = adapter.new_connection(None).expect("connection");
    let ctx = dbt_xdbc::QueryCtx::new("duckdb".to_string());

    // Create a table and insert a row
    let create = ctx.with_sql("create table main.rtt(x integer, y text)");
    adapter.exec_stmt(&mut *conn, &create, false).expect("create ok");
    let insert = ctx.with_sql("insert into main.rtt values (1, 'a')");
    adapter.exec_stmt(&mut *conn, &insert, false).expect("insert ok");

    // Locate the relation
    let found = adapter
        .get_relation(&ctx, &mut *conn, "", "main", "rtt")
        .expect("get_relation ok");
    assert!(found.is_some());
    let relation = found.unwrap();

    // Validate FQN renders with detected catalog (in-memory => memory)
    let fqn = relation.render_self_as_str();
    let parts: Vec<&str> = fqn.split('.').collect();
    assert_eq!(parts.len(), 3, "expected three-part name, got: {}", fqn);
    assert_eq!(parts[0].to_ascii_lowercase(), "memory");

    // Retrieve columns via typed adapter
    let mut env = minijinja::Environment::new();
    let state = env.empty_state();
    let cols = adapter
        .get_columns_in_relation(&state, relation.clone())
        .expect("cols ok");
    let names: Vec<String> = cols.iter().map(|c| c.name().to_string()).collect();
    assert!(names.contains(&"x".to_string()));
    assert!(names.contains(&"y".to_string()));

    // Also verify we can construct relation explicitly and render FQN
        let rel2 = create_relation(
            adapter.adapter_type(),
            "main".to_string(),
            "main".to_string(),
            Some("rtt".to_string()),
            None,
            adapter.quoting(),
    )
    .expect("create_relation ok");
    assert!(rel2.render_self_as_str().contains("main.rtt"));
}
