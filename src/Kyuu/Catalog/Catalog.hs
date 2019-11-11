{-# LANGUAGE FlexibleContexts #-}
module Kyuu.Catalog.Catalog
        ( CatalogState(..)
        , initCatalogState
        , lookupTableIdByName
        , getTableColumns
        , lookupTableColumnById
        , lookupTableColumnByName
        , createTableWithCatalog
        )
where

import           Kyuu.Prelude
import           Kyuu.Core
import           Kyuu.Catalog.Schema
import           Kyuu.Catalog.State
import           Kyuu.Error

import           Control.Lens

import           Data.List                      ( find )
import           Data.Maybe                     ( fromJust )
import           Data.Map                       ( Map )
import qualified Data.Map                      as Map

lookupTableById :: (HasKState m) => OID -> m (Maybe TableSchema)
lookupTableById tId = do
        state <- getCatalogState
        return $ find
                (\TableSchema { tableId = tableId } -> tableId == tId)
                (state ^. tableSchemas_)

lookupTableIdByName :: (HasKState m) => String -> m (Maybe OID)
lookupTableIdByName name = do
        state <- getCatalogState
        let schema = find
                    (\TableSchema { tableName = tableName } -> tableName == name
                    )
                    (state ^. tableSchemas_)
        return $ fmap tableId schema

getTableColumns :: (HasKState m) => OID -> m [ColumnSchema]
getTableColumns tId = do
        state <- getCatalogState
        let schema = fromJust $ find
                    (\TableSchema { tableId = tableId } -> tableId == tId)
                    (state ^. tableSchemas_)
        return $ tableCols schema

lookupTableColumnById :: (HasKState m) => OID -> OID -> m (Maybe ColumnSchema)
lookupTableColumnById tId cId = do
        table <- lookupTableById tId
        case table of
                Just TableSchema { tableCols = tableCols } -> return $ find
                        (\ColumnSchema { colId = colId } -> colId == cId)
                        tableCols

                _ -> return Nothing

lookupTableColumnByName
        :: (HasKState m) => OID -> String -> m (Maybe ColumnSchema)
lookupTableColumnByName tId name = do
        table <- lookupTableById tId
        case table of
                Just TableSchema { tableCols = tableCols } -> return $ find
                        (\ColumnSchema { colName = colName } -> colName == name)
                        tableCols

                _ -> return Nothing

createTableWithCatalog :: (StorageBackend m) => TableSchema -> Kyuu m ()
createTableWithCatalog (TableSchema _ tableName tableCols) = do
        exists <- lookupTableIdByName tableName
        case exists of
                Just _  -> lerror (TableExists tableName)
                Nothing -> do
                        state <- getCatalogState
                        let     tableId = 1 + length (state ^. tableSchemas_)
                                schema =
                                        TableSchema tableId tableName tableCols
                        modifyCatalogState $ over tableSchemas_ (schema :)
