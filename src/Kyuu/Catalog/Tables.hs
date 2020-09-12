{-# LANGUAGE NamedFieldPuns #-}

module Kyuu.Catalog.Tables
  ( -- pg_class
    pgClassTableId,
    pgClassTableSchema,
    classOidColNum,
    classNameColNum,
    getClassTableTuple,
    -- pg_attribute
    pgAttributeTableId,
    pgAttributeTableSchema,
    attributeRelIdColNum,
    attributeNameColNum,
    attributeTypeColNum,
    attributeColNumColNum,
    getAttributeTableTuple,
    -- pg_index
    pgIndexTableId,
    pgIndexTableSchema,
    indexRelIdColNum,
    indexIndRelIdColNum,
    getIndexTableTuple,
    -- pg_class oid index
    classOidIndexId,
    classOidIndexSchema,
    -- pg_class name index
    classNameIndexId,
    classNameIndexSchema,
    -- pg_attribute rel id index
    attributeRelIdColNumIndexId,
    attributeRelIdColNumIndexSchema,
    -- pg_index rel id index
    indexIndRelIdIndexId,
    indexIndRelIdIndexSchema,
  )
where

import Kyuu.Catalog.Schema
import Kyuu.Prelude
import Kyuu.Value

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

getClassTableTuple :: TableSchema -> Tuple
getClassTableTuple TableSchema {tableId, tableName} =
  let tupleDesc = map (\ColumnSchema {colTable, colId} -> ColumnDesc colTable colId) (tableCols pgClassTableSchema)
   in Tuple tupleDesc [VInt tableId, VString tableName]

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

getAttributeTableTuple :: OID -> ColumnSchema -> Tuple
getAttributeTableTuple tableId ColumnSchema {colId, colName, colType} =
  let tupleDesc = map (\ColumnSchema {colTable, colId} -> ColumnDesc colTable colId) (tableCols pgAttributeTableSchema)
   in Tuple tupleDesc [VInt tableId, VString colName, VInt (fromEnum colType), VInt colId]

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

getIndexTableTuple :: IndexSchema -> Tuple
getIndexTableTuple IndexSchema {indexId, indexTableId, colNums} =
  let tupleDesc = map (\ColumnSchema {colTable, colId} -> ColumnDesc colTable colId) (tableCols pgIndexTableSchema)
   in Tuple
        tupleDesc
        [ VInt indexId,
          VInt indexTableId,
          VInt (length colNums),
          VIntList colNums
        ]

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
