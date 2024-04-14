//! Schemas that appear through ADBC.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, UnionFields, UnionMode};
use once_cell::sync::Lazy;

/// Schema of the data returned by [crate::Connection::get_table_types].
pub static GET_TABLE_TYPES_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "table_type",
        DataType::Utf8,
        false,
    )]))
});

/// Schema of the data returned by [crate::Connection::get_info].
pub static GET_INFO_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    let info_schema = DataType::Union(
        UnionFields::new(
            vec![0, 1, 2, 3, 4, 5],
            vec![
                Field::new("string_value", DataType::Utf8, true),
                Field::new("bool_value", DataType::Boolean, true),
                Field::new("int64_value", DataType::Int64, true),
                Field::new("int32_bitmask", DataType::Int32, true),
                Field::new_list(
                    "string_list",
                    Field::new_list_field(DataType::Utf8, true),
                    true,
                ),
                Field::new_map(
                    "int32_to_int32_list_map",
                    "entries",
                    Field::new("key", DataType::Int32, false),
                    Field::new_list("value", Field::new_list_field(DataType::Int32, true), true),
                    false,
                    true,
                ),
            ],
        ),
        UnionMode::Dense,
    );

    Arc::new(Schema::new(vec![
        Field::new("info_name", DataType::UInt32, false),
        Field::new("info_value", info_schema, true),
    ]))
});

/// Schema of data returned by [crate::Connection::get_statistic_names].
pub static GET_STATISTIC_NAMES_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("statistic_name", DataType::Utf8, false),
        Field::new("statistic_key", DataType::Int16, false),
    ]))
});

/// Schema of data returned by [crate::Connection::get_statistics].
pub static GET_STATISTICS_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    let statistic_value_schema = DataType::Union(
        UnionFields::new(
            vec![0, 1, 2, 3],
            vec![
                Field::new("int64", DataType::Int64, true),
                Field::new("uint64", DataType::UInt64, true),
                Field::new("float64", DataType::Float64, true),
                Field::new("binary", DataType::Binary, true),
            ],
        ),
        UnionMode::Dense,
    );

    let statistics_schema = DataType::Struct(
        vec![
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, true),
            Field::new("statistic_key", DataType::Int16, false),
            Field::new("statistic_value", statistic_value_schema, false),
            Field::new("statistic_is_approximate", DataType::Boolean, false),
        ]
        .into(),
    );

    let statistics_db_schema_schema = DataType::Struct(
        vec![
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new(
                "db_schema_statistics",
                DataType::new_list(statistics_schema, true),
                false,
            ),
        ]
        .into(),
    );

    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new(
            "catalog_db_schemas",
            DataType::new_list(statistics_db_schema_schema, true),
            false,
        ),
    ]))
});

/// Schema of data returned by [crate::Connection::get_objects].
pub static GET_OBJECTS_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    let usage_schema = DataType::Struct(
        vec![
            Field::new("fk_catalog", DataType::Utf8, true),
            Field::new("fk_db_schema", DataType::Utf8, true),
            Field::new("fk_table", DataType::Utf8, false),
            Field::new("fk_column_name", DataType::Utf8, false),
        ]
        .into(),
    );

    let constraint_schema = DataType::Struct(
        vec![
            Field::new("constraint_name", DataType::Utf8, true),
            Field::new("constraint_type", DataType::Utf8, false),
            Field::new(
                "constraint_column_names",
                DataType::new_list(DataType::Utf8, true),
                false,
            ),
            Field::new(
                "constraint_column_usage",
                DataType::new_list(usage_schema, true),
                true,
            ),
        ]
        .into(),
    );

    let column_schema = DataType::Struct(
        vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int32, true),
            Field::new("remarks", DataType::Utf8, true),
            Field::new("xdbc_data_type", DataType::Int16, true),
            Field::new("xdbc_type_name", DataType::Utf8, true),
            Field::new("xdbc_column_size", DataType::Int32, true),
            Field::new("xdbc_decimal_digits", DataType::Int16, true),
            Field::new("xdbc_num_prec_radix", DataType::Int16, true),
            Field::new("xdbc_nullable", DataType::Int16, true),
            Field::new("xdbc_column_def", DataType::Utf8, true),
            Field::new("xdbc_sql_data_type", DataType::Int16, true),
            Field::new("xdbc_datetime_sub", DataType::Int16, true),
            Field::new("xdbc_char_octet_length", DataType::Int32, true),
            Field::new("xdbc_is_nullable", DataType::Utf8, true),
            Field::new("xdbc_scope_catalog", DataType::Utf8, true),
            Field::new("xdbc_scope_schema", DataType::Utf8, true),
            Field::new("xdbc_scope_table", DataType::Utf8, true),
            Field::new("xdbc_is_autoincrement", DataType::Boolean, true),
            Field::new("xdbc_is_generatedcolumn", DataType::Boolean, true),
        ]
        .into(),
    );

    let table_schema = DataType::Struct(
        vec![
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
            Field::new(
                "table_columns",
                DataType::new_list(column_schema, true),
                true,
            ),
            Field::new(
                "table_constraints",
                DataType::new_list(constraint_schema, true),
                true,
            ),
        ]
        .into(),
    );

    let db_schema_schema = DataType::Struct(
        vec![
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new(
                "db_schema_tables",
                DataType::new_list(table_schema, true),
                true,
            ),
        ]
        .into(),
    );

    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new(
            "catalog_db_schemas",
            DataType::new_list(db_schema_schema, true),
            true,
        ),
    ]))
});
