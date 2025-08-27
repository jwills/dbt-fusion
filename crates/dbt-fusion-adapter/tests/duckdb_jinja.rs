use std::collections::HashMap;
use std::sync::Arc;

use dbt_auth::AdapterConfig;
use dbt_fusion_adapter::BridgeAdapter;
use dbt_fusion_adapter::BaseAdapter;
use dbt_fusion_adapter::cache::RelationCache;
use dbt_fusion_adapter::duckdb::adapter::DuckdbAdapter;
use dbt_fusion_adapter::SqlEngine;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_xdbc::Backend;

#[test]
fn duckdb_jinja_adapter_execute() {
    if std::env::var("DUCKDB_DRIVER_PATH").is_err() && std::env::var("DUCKDB_DRIVER_NAME").is_err() {
        eprintln!("skipping jinja test: DUCKDB_DRIVER_PATH/NAME not set");
        return;
    }

    // Build engine + typed adapter + bridge
    let adapter_cfg = AdapterConfig::new(HashMap::new());
    let auth = dbt_auth::auth_for_backend(Backend::DuckDb);
    let token = dbt_common::cancellation::CancellationToken::never_cancels();
    let engine = SqlEngine::new(Arc::from(auth), adapter_cfg, token);
    let quoting = ResolvedQuoting { database: false, schema: false, identifier: false };
    let typed = Arc::new(DuckdbAdapter::new(engine.clone(), quoting, None));
    let cache = Arc::new(RelationCache::default());
    let bridge = Arc::new(BridgeAdapter::new(typed, None, cache));

    // Build a minimal minijinja env with adapter global
    let mut env = minijinja::Environment::new();
    env.add_global("adapter", bridge.as_value());
    env.add_global("dialect", minijinja::Value::from("duckdb"));

    // Execute simple query via Jinja adapter call
    let tpl = "{% set _res = adapter.execute('select 1 as x', fetch=true) %}ok";
    let rendered = env.render_named_str("test.sql", tpl, HashMap::<String, minijinja::Value>::new(), &[])
        .expect("render ok");
    assert_eq!(rendered, "ok");
}
