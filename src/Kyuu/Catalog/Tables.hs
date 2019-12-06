module Kyuu.Catalog.Tables
        ( pgClassTableId
        , pgClassTableSchema
        , pgAttributeTableId
        , pgAttributeTableSchema
        , pgIndexTableId
        , pgIndexTableSchema
        , classOidIndexId
        , classOidIndexSchema
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

defineIndex :: OID -> OID -> Int -> IndexSchema
defineIndex = IndexSchema

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

pgIndexTableId :: OID
pgIndexTableId = 3

pgIndexTableSchema :: TableSchema
pgIndexTableSchema = defineTable
        pgIndexTableId
        "pg_index"
        [_c "indexrelid" SInt, _c "indrelid" SInt, _c "indatts" SInt]


classOidIndexId :: OID
classOidIndexId = 4

classOidIndexSchema :: IndexSchema
classOidIndexSchema = defineIndex classOidIndexId pgClassTableId 1
