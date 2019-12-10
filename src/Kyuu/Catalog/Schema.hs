{-# LANGUAGE DuplicateRecordFields #-}
module Kyuu.Catalog.Schema
        ( SchemaType(..)
        , ColumnSchema(..)
        , TableSchema(..)
        , IndexSchema(..)
        )
where

import           Kyuu.Prelude

data SchemaType = SNull
                | SInt
                | SFloat
                | SDouble
                | SString
                | SIntList
                deriving (Eq, Show)

data ColumnSchema = ColumnSchema { colTable :: OID
                                 , colId :: OID
                                 , colName :: String
                                 , colType :: SchemaType }
                  deriving (Eq, Show)

data TableSchema = TableSchema { tableId :: OID
                               , tableName :: String
                               , tableCols :: [ColumnSchema]}
                 deriving (Eq, Show)

data IndexSchema = IndexSchema { indexId :: OID
                               , indexTableId :: OID
                               , colNums :: [Int]
                               }
                 deriving (Eq, Show)
