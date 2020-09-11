module Kyuu.Catalog.Tables
  ( pgClassTableId,
    pgClassTableSchema,
    classOidColNum,
    classNameColNum,
    pgAttributeTableId,
    pgAttributeTableSchema,
    attributeRelIdColNum,
    attributeNameColNum,
    attributeTypeColNum,
    attributeColNumColNum,
    pgIndexTableId,
    pgIndexTableSchema,
    indexRelIdColNum,
    indexIndRelIdColNum,
    classOidIndexId,
    classOidIndexSchema,
    classNameIndexId,
    classNameIndexSchema,
    attributeRelIdColNumIndexId,
    attributeRelIdColNumIndexSchema,
    indexIndRelIdIndexId,
    indexIndRelIdIndexSchema,
  )
where

import Kyuu.Catalog.Schema
import Kyuu.Prelude

_c :: String -> SchemaType -> ColumnSchema
_c = ColumnSchema 0 0

defineTable :: OID -> String -> [ColumnSchema] -> TableSchema
defineTable tId name cols = TableSchema tId name columns
  where
    columns =
      map
        ( \(colId, ColumnSchema _ _ name typ) ->
            ColumnSchema tId colId name typ
        )
        $ zip [1 ..] cols

defineIndex :: OID -> OID -> [Int] -> IndexSchema
defineIndex = IndexSchema

pgClassTableId :: OID
pgClassTableId = 1

pgClassTableSchema :: TableSchema
pgClassTableSchema =
  defineTable
    pgClassTableId
    "pg_class"
    [_c "oid" SInt, _c "relname" SString]

classOidColNum :: Int
classOidColNum = 1

classNameColNum :: Int
classNameColNum = 2

pgAttributeTableId :: OID
pgAttributeTableId = 2

pgAttributeTableSchema :: TableSchema
pgAttributeTableSchema =
  defineTable
    pgAttributeTableId
    "pg_attribute"
    [ _c "attrelid" SInt,
      _c "attname" SString,
      _c "atttypid" SInt,
      _c "attnum" SInt
    ]

attributeRelIdColNum :: Int
attributeRelIdColNum = 1

attributeNameColNum :: Int
attributeNameColNum = 2

attributeTypeColNum :: Int
attributeTypeColNum = 3

attributeColNumColNum :: Int
attributeColNumColNum = 4

pgIndexTableId :: OID
pgIndexTableId = 3

pgIndexTableSchema :: TableSchema
pgIndexTableSchema =
  defineTable
    pgIndexTableId
    "pg_index"
    [ _c "indexrelid" SInt,
      _c "indrelid" SInt,
      _c "indatts" SInt,
      _c "indkey" SIntList
    ]

indexRelIdColNum :: Int
indexRelIdColNum = 1

indexIndRelIdColNum :: Int
indexIndRelIdColNum = 2

classOidIndexId :: OID
classOidIndexId = 4

classOidIndexSchema :: IndexSchema
classOidIndexSchema =
  defineIndex classOidIndexId pgClassTableId [classOidColNum]

classNameIndexId :: OID
classNameIndexId = 5

classNameIndexSchema :: IndexSchema
classNameIndexSchema =
  defineIndex classNameIndexId pgClassTableId [classNameColNum]

attributeRelIdColNumIndexId :: OID
attributeRelIdColNumIndexId = 6

attributeRelIdColNumIndexSchema :: IndexSchema
attributeRelIdColNumIndexSchema =
  defineIndex
    attributeRelIdColNumIndexId
    pgAttributeTableId
    [attributeRelIdColNum, attributeColNumColNum]

indexIndRelIdIndexId :: OID
indexIndRelIdIndexId = 7

indexIndRelIdIndexSchema =
  defineIndex
    indexIndRelIdIndexId
    pgIndexTableId
    [indexIndRelIdColNum]
