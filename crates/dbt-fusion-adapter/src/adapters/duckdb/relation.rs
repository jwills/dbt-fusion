use crate::adapters::information_schema::InformationSchema;
use crate::adapters::relation_object::{RelationObject, StaticBaseRelation};
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::relations::base::{
    BaseRelation, BaseRelationProperties, Policy, RelationPath, TableFormat,
};
use minijinja::{
    arg_utils::ArgParser, Error as MinijinjaError, State, Value,
};

use std::any::Any;
use std::sync::Arc;

/// A struct representing the DuckDB relation type for use with static methods
#[derive(Clone, Debug)]
pub struct DuckDBRelationType;

impl StaticBaseRelation for DuckDBRelationType {
    fn try_new(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: ResolvedQuoting,
    ) -> Result<Value, MinijinjaError> {
        Ok(RelationObject::new(Arc::new(DuckDBRelation::new(
            database,
            schema,
            identifier,
            relation_type,
            TableFormat::Default,
            custom_quoting,
        )))
        .into_value())
    }

    fn create(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args = ArgParser::new(args, None);
        let database: Option<String> = args.get("database").ok();
        let schema: Option<String> = args.get("schema").ok();
        let identifier: Option<String> = args.get("identifier").ok();
        let relation_type: Option<String> = args.get("type").ok();

        self.try_new(
            database,
            schema,
            identifier,
            relation_type.map(|s| RelationType::from(s.as_str())),
            // DuckDB uses double quotes like PostgreSQL
            ResolvedQuoting {
                database: false,
                schema: false,
                identifier: false,
            },
        )
    }

    fn get_adapter_type(&self) -> String {
        "duckdb".to_string()
    }
}

/// A struct representing a DuckDB relation
#[derive(Clone, Debug)]
pub struct DuckDBRelation {
    /// The path of the relation
    pub path: RelationPath,
    /// The relation type (default: None)
    pub relation_type: Option<RelationType>,
    /// The table format of the relation
    pub table_format: TableFormat,
    /// Include policy
    pub include_policy: Policy,
    /// Quote policy
    pub quote_policy: Policy,
}

impl BaseRelationProperties for DuckDBRelation {
    fn quote_policy(&self) -> Policy {
        self.quote_policy
    }

    fn include_policy(&self) -> Policy {
        self.include_policy
    }

    fn quote_character(&self) -> char {
        '"' // DuckDB uses double quotes like PostgreSQL
    }
}

impl DuckDBRelation {
    /// Creates a new DuckDB relation
    pub fn new(
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        table_format: TableFormat,
        custom_quoting: ResolvedQuoting,
    ) -> Self {
        Self {
            path: RelationPath {
                database,
                schema,
                identifier,
            },
            relation_type,
            table_format,
            include_policy: Policy::enabled(),
            // DuckDB follows PostgreSQL-like quoting behavior
            quote_policy: custom_quoting,
        }
    }
}

impl BaseRelation for DuckDBRelation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn information_schema_inner(
        &self,
        database: Option<String>,
        view_name: &str,
    ) -> Result<Value, MinijinjaError> {
        let result = InformationSchema::try_from_relation(database, view_name)?;
        Ok(RelationObject::new(Arc::new(result)).into_value())
    }

    /// Creates a new DuckDB relation from a state and a list of values
    fn create_from(&self, _: &State, _: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("create_from not yet implemented for DuckDB")
    }

    /// Returns the database name
    fn database(&self) -> Value {
        Value::from(self.path.database.clone())
    }

    /// Returns the schema name
    fn schema(&self) -> Value {
        Value::from(self.path.schema.clone())
    }

    /// Returns the identifier name
    fn identifier(&self) -> Value {
        Value::from(self.path.identifier.clone())
    }

    /// Helper: is this relation renamable?
    fn can_be_renamed(&self) -> bool {
        matches!(
            self.relation_type(),
            Some(RelationType::Table) | Some(RelationType::View)
        )
    }

    /// Helper: is this relation replaceable?
    fn can_be_replaced(&self) -> bool {
        matches!(
            self.relation_type(),
            Some(RelationType::Table) | Some(RelationType::View)
        )
    }

    fn quoted(&self, s: &str) -> String {
        format!("\"{}\"", s)
    }

    /// Returns the relation type
    fn relation_type(&self) -> Option<RelationType> {
        self.relation_type.clone()
    }

    fn as_value(&self) -> Value {
        RelationObject::new(Arc::new(self.clone())).into_value()
    }

    fn adapter_type(&self) -> Option<String> {
        Some("duckdb".to_string())
    }

    fn needs_to_drop(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        let value = parser.get::<Value>("old_relation").unwrap();

        if let Some(old_relation) = value.downcast_object_ref::<DuckDBRelation>() {
            if old_relation.is_table() {
                // DuckDB tables can be replaced without dropping
                Ok(Value::from(false))
            } else {
                // An existing view must be dropped for model to build into a table
                Ok(Value::from(true))
            }
        } else {
            Ok(Value::from(false))
        }
    }

    /// Returns the appropriate DDL prefix for creating a table
    fn get_ddl_prefix_for_create(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut arg_parser = ArgParser::new(args, None);
        let _config = arg_parser.get::<Value>("config").unwrap();
        let temporary = arg_parser.get::<bool>("temporary").unwrap();

        if temporary {
            return Ok(Value::from("temporary"));
        }

        // DuckDB doesn't have complex table prefixes like Snowflake
        Ok(Value::from(""))
    }

    fn get_ddl_prefix_for_alter(&self) -> Result<Value, MinijinjaError> {
        // DuckDB doesn't need special prefixes for ALTER statements
        Ok(Value::from(""))
    }

    fn get_iceberg_ddl_options(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        // DuckDB doesn't support Iceberg format
        Ok(Value::from(""))
    }

    fn include_inner(&self, policy: Policy) -> Result<Value, MinijinjaError> {
        let mut relation = self.clone();
        relation.include_policy = policy;

        Ok(relation.as_value())
    }

    fn normalize_component(&self, component: &str) -> String {
        // DuckDB is case-sensitive by default, no normalization needed
        component.to_string()
    }

    fn create_relation(
        &self,
        database: String,
        schema: String,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Policy,
    ) -> Result<Arc<dyn BaseRelation>, MinijinjaError> {
        Ok(Arc::new(DuckDBRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            TableFormat::Default,
            custom_quoting,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::dbt_types::RelationType;

    #[test]
    fn test_try_new_via_static_base_relation() {
        let relation_type = DuckDBRelationType;
        let relation = relation_type
            .try_new(
                Some("d".to_string()),
                Some("s".to_string()),
                Some("i".to_string()),
                Some(RelationType::Table),
                ResolvedQuoting {
                    database: true,
                    schema: true,
                    identifier: true,
                },
            )
            .unwrap();

        let relation = relation.downcast_object::<RelationObject>().unwrap();
        assert_eq!(
            relation.inner().render_self().unwrap().as_str().unwrap(),
            r#""d"."s"."i""#
        );
        assert_eq!(relation.relation_type().unwrap(), RelationType::Table);
    }

    #[test]
    fn test_duckdb_relation_properties() {
        let relation = DuckDBRelation::new(
            Some("test_db".to_string()),
            Some("test_schema".to_string()),
            Some("test_table".to_string()),
            Some(RelationType::Table),
            TableFormat::Default,
            ResolvedQuoting {
                database: false,
                schema: false,
                identifier: false,
            },
        );

        assert_eq!(relation.quote_character(), '"');
        assert_eq!(relation.adapter_type(), Some("duckdb".to_string()));
        assert!(relation.can_be_renamed());
        assert!(relation.can_be_replaced());
    }
}