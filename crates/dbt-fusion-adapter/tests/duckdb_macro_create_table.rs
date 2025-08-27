use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use dbt_auth::AdapterConfig;
use dbt_fusion_adapter::cache::RelationCache;
use dbt_fusion_adapter::duckdb::adapter::DuckdbAdapter;
use dbt_fusion_adapter::relation_object::create_relation;
use dbt_fusion_adapter::{BaseAdapter, BridgeAdapter, SqlEngine};
use dbt_fusion_adapter::AdapterTyping;
use dbt_fusion_adapter::TypedBaseAdapter;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_xdbc::Backend;
use minijinja::{Environment, Value};
use minijinja::constants::DBT_AND_ADAPTERS_NAMESPACE;
use minijinja::value::ValueMap;

#[test]
fn duckdb_macro_create_table_executes() {
    if std::env::var("DUCKDB_DRIVER_PATH").is_err() && std::env::var("DUCKDB_DRIVER_NAME").is_err() {
        eprintln!("skipping macro create_table test: DUCKDB_DRIVER_PATH/NAME not set");
        return;
    }

    // Load macro file
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf();
    let adapters_path = repo_root
        .join("crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/adapters.sql");
    if !adapters_path.exists() {
        eprintln!("skipping: adapters.sql not found at {}", adapters_path.display());
        return;
    }
    let adapters_sql = std::fs::read_to_string(&adapters_path).expect("read adapters.sql");

    // Adapter + bridge
    let auth = dbt_auth::auth_for_backend(Backend::DuckDb);
    let cfg = AdapterConfig::new(HashMap::new());
    let token = dbt_common::cancellation::CancellationToken::never_cancels();
    let engine = SqlEngine::new(Arc::from(auth), cfg, token);
    let quoting = ResolvedQuoting { database: false, schema: false, identifier: false };
    let typed = Arc::new(DuckdbAdapter::new(engine.clone(), quoting, None));
    let cache = Arc::new(RelationCache::default());
    let bridge = Arc::new(BridgeAdapter::new(typed.clone(), None, cache));

    // Relation for target table
    let relation = create_relation(
        typed.adapter_type().to_string(),
        "".to_string(),
        "main".to_string(),
        Some("t_create_macro".to_string()),
        None,
        typed.quoting(),
    ).expect("relation");

    // Build Jinja env: adapter, dialect, macro template, namespace mapping
    let mut env = Environment::new();
    env.add_global("adapter", bridge.as_value());
    env.add_global("dialect", Value::from("duckdb"));
    env.add_global("relation", relation.as_value());
    // Register adapters.sql under a template name that ends with the macro name we will call.
    // We need the template name to end with 'duckdb__create_table_as' for dispatch to locate the function.
    // To keep it simple, register a duplicate template pointing to the same content.
    env.add_template_owned(
        "dbt_duckdb.duckdb__create_table_as",
        adapters_sql.clone(),
        Some(adapters_path.to_string_lossy().to_string()),
        &[],
    ).expect("add template");
    // Map macro name to package for namespace resolution
    let mut map = ValueMap::new();
    map.insert(Value::from("duckdb__create_table_as"), Value::from("dbt_duckdb"));
    env.add_global(DBT_AND_ADAPTERS_NAMESPACE, Value::from_object(map));

    // Render macro to produce SQL, then execute via adapter
    let tpl = "{{ adapter.dispatch('create_table_as', 'dbt')(false, relation, 'select 7 as n', 'sql') }}";
    let rendered = env
        .render_named_str("create_table_as_test.sql", tpl, HashMap::<String, Value>::new(), &[])
        .expect("render ok");

    // Execute the emitted SQL and verify
    let mut conn = typed.new_connection(None).expect("conn");
    let ctx = dbt_xdbc::QueryCtx::new("duckdb").with_sql(rendered);
    let _resp = typed.exec_stmt(&mut *conn, &ctx, false).expect("exec create table");

    // Select from the table to verify it exists
    let ctx2 = dbt_xdbc::QueryCtx::new("duckdb").with_sql("select * from main.t_create_macro");
    let (_resp2, table) = typed.query(&mut *conn, &ctx2, None).expect("select ok");
    assert_eq!(table.num_rows(), 1);
}
