//! Schemas that appear through ADBC.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, UnionFields, UnionMode};
use once_cell::sync::Lazy;

/// Schema of the data returned by [crate::Connection::get_table_types].
pub static GET_TABLE_TYPES: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "table_type",
        DataType::Utf8,
        false,
    )]))
});

/// Schema of the data returned by [crate::Connection::get_info].
pub static GET_INFO_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    let union_fields = UnionFields::new(
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
    );
    Arc::new(Schema::new(vec![
        Field::new("info_name", DataType::UInt32, false),
        Field::new(
            "info_value",
            DataType::Union(union_fields, UnionMode::Dense),
            true,
        ),
    ]))
});
