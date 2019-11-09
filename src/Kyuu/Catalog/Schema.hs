{-# LANGUAGE DuplicateRecordFields #-}
module Kyuu.Catalog.Schema
        ( SchemaType(..)
        , ColumnSchema(..)
        , TableSchema(..)
        )
where

import           Kyuu.Prelude

data SchemaType = SNull
                | SInt
                | SFloat
                | SDouble
                | SString
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
