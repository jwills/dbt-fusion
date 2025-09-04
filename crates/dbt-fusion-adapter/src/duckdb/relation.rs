use crate::information_schema::InformationSchema;
use crate::relation_object::{RelationObject, StaticBaseRelation};

use dbt_common::{ErrorCode, FsResult, current_function_name, fs_err};
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::relations::base::{
    BaseRelation, BaseRelationProperties, Policy, RelationPath,
};
use minijinja::arg_utils::{ArgParser, check_num_args};
use minijinja::{Error as MinijinjaError, State, Value};

use std::any::Any;
use std::sync::Arc;

const MAX_CHARACTERS_IN_IDENTIFIER: usize = 63; // conservative default (align with Postgres)

/// A struct representing the DuckDB relation type for use with static methods
#[derive(Clone, Debug)]
pub struct DuckdbRelationType(pub ResolvedQuoting);

impl StaticBaseRelation for DuckdbRelationType {
    fn try_new(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Option<ResolvedQuoting>,
    ) -> Result<Value, MinijinjaError> {
        Ok(RelationObject::new(Arc::new(DuckdbRelation::try_new(
            database,
            schema,
            identifier,
            relation_type,
            custom_quoting.unwrap_or(self.0),
        )?))
        .into_value())
    }

    fn get_adapter_type(&self) -> String {
        "duckdb".to_string()
    }
}

/// A relation object for DuckDB adapter
#[derive(Clone, Debug)]
pub struct DuckdbRelation {
    /// The database, schema, and identifier of the relation
    pub path: RelationPath,
    /// The relation type
    pub relation_type: Option<RelationType>,
    /// Include policy
    pub include_policy: Policy,
    /// Quote policy
    pub quote_policy: Policy,
}

impl DuckdbRelation {
    /// Creates a new DuckDB relation
    pub fn try_new(
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: ResolvedQuoting,
    ) -> Result<Self, MinijinjaError> {
        Self::try_new_with_policy(
            RelationPath {
                database,
                schema,
                identifier,
            },
            relation_type,
            Policy::enabled(),
            custom_quoting,
        )
    }

    /// Creates a new DuckDB relation
    pub fn try_new_with_policy(
        path: RelationPath,
        relation_type: Option<RelationType>,
        include_policy: Policy,
        quote_policy: Policy,
    ) -> Result<Self, MinijinjaError> {
        // Use a conservative identifier length check (matches Postgres). Adjust later if needed.
        if path.identifier.is_some()
            && path.identifier.as_deref().map(|i| i.len()).unwrap() > MAX_CHARACTERS_IN_IDENTIFIER
            && relation_type.is_some()
        {
            return Err(MinijinjaError::new(
                minijinja::ErrorKind::InvalidOperation,
                format!(
                    "Relation name '{:?}' is longer than {} characters",
                    path.identifier, MAX_CHARACTERS_IN_IDENTIFIER
                ),
            ));
        }

        Ok(Self {
            path,
            relation_type,
            include_policy,
            quote_policy,
        })
    }
}

impl BaseRelationProperties for DuckdbRelation {
    fn include_policy(&self) -> Policy {
        self.include_policy
    }

    fn quote_policy(&self) -> Policy {
        self.quote_policy
    }

    fn quote_character(&self) -> char {
        '"'
    }
    fn get_database(&self) -> FsResult<String> {
        self.path.database.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "database is required for duckdb relation",
            )
        })
    }

    fn get_schema(&self) -> FsResult<String> {
        self.path.schema.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "schema is required for duckdb relation",
            )
        })
    }

    fn get_identifier(&self) -> FsResult<String> {
        self.path.identifier.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "identifier is required for duckdb relation",
            )
        })
    }
}

impl BaseRelation for DuckdbRelation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create_from(&self, _: &State, _: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("DuckDB relation creation from Jinja values")
    }

    fn database(&self) -> Value {
        Value::from(self.path.database.clone())
    }

    fn schema(&self) -> Value {
        Value::from(self.path.schema.clone())
    }

    fn identifier(&self) -> Value {
        Value::from(self.path.identifier.clone())
    }

    fn relation_type(&self) -> Option<RelationType> {
        self.relation_type
    }

    fn as_value(&self) -> Value {
        RelationObject::new(Arc::new(self.clone())).into_value()
    }

    fn adapter_type(&self) -> Option<String> {
        Some("duckdb".to_string())
    }

    fn include_inner(&self, include_policy: Policy) -> Result<Value, MinijinjaError> {
        let relation = DuckdbRelation::try_new_with_policy(
            self.path.clone(),
            self.relation_type,
            include_policy,
            self.quote_policy,
        )?;
        Ok(relation.as_value())
    }

    fn relation_max_name_length(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let args = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &args, 0, 0)?;
        Ok(Value::from(MAX_CHARACTERS_IN_IDENTIFIER))
    }

    fn normalize_component(&self, component: &str) -> String {
        component.to_lowercase()
    }

    fn create_relation(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Policy,
    ) -> Result<Arc<dyn BaseRelation>, MinijinjaError> {
        Ok(Arc::new(DuckdbRelation::try_new(
            database,
            schema,
            identifier,
            relation_type,
            custom_quoting,
        )?))
    }

    fn information_schema_inner(
        &self,
        database: Option<String>,
        view_name: &str,
    ) -> Result<Value, MinijinjaError> {
        let result = InformationSchema::try_from_relation(database, view_name)?;
        Ok(RelationObject::new(Arc::new(result)).into_value())
    }
}

