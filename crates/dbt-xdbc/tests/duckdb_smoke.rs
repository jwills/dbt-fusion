use adbc_core::error::Result;
use arrow_array::RecordBatchReader;

use dbt_xdbc::Backend;

#[test_with::env(ADBC_DUCKDB_TESTS)]
#[test]
fn duckdb_smoke_query() -> Result<()> {
    // Load DuckDB driver dynamically from system/lib folder
    let mut driver = dbt_xdbc::driver::Builder::new(Backend::DuckDb).try_load()?;
    let mut database = driver.new_database()?;

    // Create a connection
    let conn_builder = dbt_xdbc::connection::Builder::default();
    let mut conn = conn_builder.build(&mut database)?;

    // Execute a simple query
    let mut stmt = conn.new_statement()?;
    let query_ctx = dbt_xdbc::QueryCtx::new("duckdb".to_string()).with_sql("select 1 as x");
    stmt.set_sql_query(&query_ctx)?;
    let mut reader: Box<dyn RecordBatchReader + Send + '_> = stmt.execute()?;

    // Read all batches and ensure we got one row
    let mut total_rows = 0usize;
    while let Some(batch) = reader.next() {
        let batch = batch?;
        total_rows += batch.num_rows();
    }
    assert_eq!(total_rows, 1);
    Ok(())
}
