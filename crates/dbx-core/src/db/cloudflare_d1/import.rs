use crate::models::connection::DatabaseType;
use crate::table_import::{mapping_indexes_for_columns, ImportSqlBatch, TableImportColumnMapping};
use crate::transfer::generate_insert_typed;

use super::sql_limits::build_sql_batches;

pub(crate) fn build_import_insert_batches(
    rows: &[Vec<serde_json::Value>],
    source_columns: &[String],
    mappings: &[TableImportColumnMapping],
    target_column_types: &[(String, String)],
    table: &str,
    schema: &str,
    max_rows: usize,
) -> Result<Vec<ImportSqlBatch>, String> {
    let mapped = mapping_indexes_for_columns(source_columns, mappings)?;
    let columns = mapped.iter().map(|(_, target)| target.clone()).collect::<Vec<_>>();
    let column_types = columns
        .iter()
        .map(|column| {
            target_column_types
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(column))
                .map(|(_, data_type)| data_type.clone())
        })
        .collect::<Vec<_>>();
    let rows = rows
        .iter()
        .map(|row| {
            mapped
                .iter()
                .map(|(source_index, _)| row.get(*source_index).cloned().unwrap_or(serde_json::Value::Null))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    build_sql_batches(rows.len(), max_rows, "import row", |range| {
        generate_insert_typed(&columns, &column_types, &rows[range], table, schema, &DatabaseType::CloudflareD1)
    })
    .map(|batches| {
        batches.into_iter().map(|batch| ImportSqlBatch { sql: batch.sql, row_count: batch.item_count }).collect()
    })
}
