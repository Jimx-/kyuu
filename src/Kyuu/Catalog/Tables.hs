module Kyuu.Catalog.Tables
        ( pgClassTableId
        , pgClassTableSchema
        , pgAttributeTableId
        , pgAttributeTableSchema
        )
where

import           Kyuu.Prelude
import           Kyuu.Catalog.Schema

_c :: String -> SchemaType -> ColumnSchema
_c = ColumnSchema 0 0

defineTable :: OID -> String -> [ColumnSchema] -> TableSchema
defineTable tId name cols = TableSchema tId name columns
    where
        columns =
                map
                                (\(colId, ColumnSchema _ _ name typ) ->
                                        ColumnSchema tId colId name typ
                                )
                        $ zip [1 ..] cols

pgClassTableId :: OID
pgClassTableId = 1

pgClassTableSchema :: TableSchema
pgClassTableSchema = defineTable pgClassTableId
                                 "pg_class"
                                 [_c "oid" SInt, _c "relname" SString]

pgAttributeTableId :: OID
pgAttributeTableId = 2

pgAttributeTableSchema :: TableSchema
pgAttributeTableSchema = defineTable
        pgAttributeTableId
        "pg_attribute"
        [_c "attrelid" SInt, _c "attname" SString, _c "attnum" SInt]
