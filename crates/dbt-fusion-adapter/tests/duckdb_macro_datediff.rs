use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use dbt_auth::AdapterConfig;
use dbt_fusion_adapter::cache::RelationCache;
use dbt_fusion_adapter::BaseAdapter;
use dbt_fusion_adapter::duckdb::adapter::DuckdbAdapter;
use dbt_fusion_adapter::{BridgeAdapter, SqlEngine};
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_xdbc::Backend;
use minijinja::macro_unit::{MacroInfo, MacroUnit};
use minijinja::{Environment, Value};
use minijinja::value::ValueMap;
use minijinja::constants::DBT_AND_ADAPTERS_NAMESPACE;

#[test]
fn duckdb_macro_datediff_renders() {
    if std::env::var("DUCKDB_DRIVER_PATH").is_err() && std::env::var("DUCKDB_DRIVER_NAME").is_err() {
        eprintln!("skipping macro test: DUCKDB_DRIVER_PATH/NAME not set");
        return;
    }

    // Locate the copied macro file
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf();
    let datediff_path = repo_root
        .join("crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/utils/datediff.sql");
    if !datediff_path.exists() {
        eprintln!("skipping: datediff macro not found at {}", datediff_path.display());
        return;
    }

    // Read macro content
    let sql = fs::read_to_string(&datediff_path).expect("read datediff.sql");

    // Create a macro unit and register the template directly in a fresh environment
    let info = MacroInfo {
        name: "duckdb__datediff".to_string(),
        path: PathBuf::from("utils/datediff.sql"),
        span: Default::default(),
        funcsign: None,
        args: vec![],
    };
    let macro_unit = MacroUnit { info, sql };

    // Build adapter
    let adapter_cfg = AdapterConfig::new(HashMap::new());
    let auth = dbt_auth::auth_for_backend(Backend::DuckDb);
    let token = dbt_common::cancellation::CancellationToken::never_cancels();
    let engine = SqlEngine::new(Arc::from(auth), adapter_cfg, token);
    let quoting = ResolvedQuoting { database: false, schema: false, identifier: false };
    let typed = Arc::new(DuckdbAdapter::new(engine.clone(), quoting, None));
    let cache = Arc::new(RelationCache::default());
    let bridge = Arc::new(BridgeAdapter::new(typed, None, cache));

    // Build a bare minijinja environment and register globals/templates needed for dispatch
    let mut env = Environment::new();
    // Adapter object and dialect
    env.add_global("adapter", bridge.as_value());
    env.add_global("dialect", Value::from("duckdb"));
    // Register the macro template under "dbt_duckdb.duckdb__datediff"
    let template_name = format!("{}.{},", "dbt_duckdb", macro_unit.info.name).trim_end_matches(',').to_string();
    env.add_template_owned(
        template_name.clone(),
        macro_unit.sql.clone(),
        Some(macro_unit.info.path.to_string_lossy().to_string()),
        &[],
    ).expect("add template");
    // Set dbt/adapters namespace so adapter.dispatch('datediff','dbt') resolves to dbt_duckdb
    let mut map = ValueMap::new();
    map.insert(Value::from("duckdb__datediff"), Value::from("dbt_duckdb"));
    env.add_global(DBT_AND_ADAPTERS_NAMESPACE, Value::from_object(map));

    // Render a template using adapter.dispatch to resolve duckdb__datediff
    let tpl = "{{ adapter.dispatch('datediff', 'dbt')(first_date='2020-01-01', second_date='2020-01-02', datepart='day') }}";
    let rendered = env
        .render_named_str("datediff_test.sql", tpl, HashMap::<String, minijinja::Value>::new(), &[])
        .expect("render ok");

    // The macro renders a SQL expression referencing date_diff
    assert!(rendered.contains("date_diff('day'"));
}
