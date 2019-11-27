{-# LANGUAGE FlexibleContexts, NamedFieldPuns, DuplicateRecordFields #-}
module Kyuu.Catalog.Catalog
        ( CatalogState(..)
        , initCatalogState
        , bootstrapCatalog
        , lookupTableById
        , lookupTableIdByName
        , getTableColumns
        , lookupTableColumnById
        , lookupTableColumnByName
        , createTableWithCatalog
        , openTable
        )
where

import           Kyuu.Prelude
import           Kyuu.Core
import           Kyuu.Catalog.Schema
import           Kyuu.Catalog.State
import           Kyuu.Catalog.Tables
import           Kyuu.Error
import           Kyuu.Table
import           Kyuu.Value
import qualified Kyuu.Storage.Backend          as S

import           Control.Lens

import           Data.List                      ( find )
import           Data.Maybe                     ( fromJust )
import           Data.Map                       ( Map )
import qualified Data.Map                      as Map

catalogDatabaseId :: OID
catalogDatabaseId = 1

lookupTableById :: (StorageBackend m) => OID -> Kyuu m (Maybe TableSchema)
lookupTableById tId = do
        state <- getCatalogState
        return $ find (\TableSchema { tableId } -> tableId == tId)
                      (state ^. tableSchemas_)

lookupTableIdByName :: (StorageBackend m) => String -> Kyuu m (Maybe OID)
lookupTableIdByName name = do
        state <- getCatalogState
        let schema = find
                    (\TableSchema { tableName = tableName } -> tableName == name
                    )
                    (state ^. tableSchemas_)
        return $ fmap (\TableSchema { tableId } -> tableId) schema

getTableColumns :: (StorageBackend m) => OID -> Kyuu m [ColumnSchema]
getTableColumns tId = do
        state <- getCatalogState
        let schema = fromJust $ find
                    (\TableSchema { tableId = tableId } -> tableId == tId)
                    (state ^. tableSchemas_)
        return $ tableCols schema

lookupTableColumnById
        :: (StorageBackend m) => OID -> OID -> Kyuu m (Maybe ColumnSchema)
lookupTableColumnById tId cId = do
        table <- lookupTableById tId
        case table of
                Just TableSchema { tableCols = tableCols } -> return $ find
                        (\ColumnSchema { colId = colId } -> colId == cId)
                        tableCols

                _ -> return Nothing

lookupTableColumnByName
        :: (StorageBackend m) => OID -> String -> Kyuu m (Maybe ColumnSchema)
lookupTableColumnByName tId name = do
        table <- lookupTableById tId
        case table of
                Just TableSchema { tableCols = tableCols } -> return $ find
                        (\ColumnSchema { colName = colName } -> colName == name)
                        tableCols

                _ -> return Nothing

createTableWithCatalog :: (StorageBackend m) => TableSchema -> Kyuu m (Table m)
createTableWithCatalog (TableSchema _ tableName tableCols) = do
        exists <- lookupTableIdByName tableName
        case exists of
                Just _  -> lerror (TableExists tableName)
                Nothing -> do
                        state <- getCatalogState
                        let     tableId = 1 + length (state ^. tableSchemas_)
                                schema  = TableSchema
                                        tableId
                                        tableName
                                        (map
                                                (\x -> x { colTable = tableId })
                                                tableCols
                                        )
                        modifyCatalogState $ over tableSchemas_ (schema :)
                        storage <- S.createTable 0 tableId
                        return $ Table tableId schema storage


openTable :: (StorageBackend m) => OID -> Kyuu m (Maybe (Table m))
openTable tableId = do
        schema <- lookupTableById tableId

        case schema of
                Nothing       -> return Nothing
                (Just schema) -> do
                        storage <- S.openTable tableId
                        case storage of
                                (Just storage) ->
                                        return
                                                $ Just
                                                          (Table tableId
                                                                 schema
                                                                 storage
                                                          )
                                _ ->
                                        lerror
                                                $ InvalidState
                                                          ("storage not created for table "
                                                          ++ show
                                                                     tableId
                                                          )

bootstrapCatalog :: (StorageBackend m) => Kyuu m ()
bootstrapCatalog = do
        let systemTables = [pgClassTableSchema, pgAttributeTableSchema]
        forM_ systemTables $ \schema ->
                modifyCatalogState $ over tableSchemas_ (schema :)

        pgClassStorage <- S.openTable pgClassTableId
        case pgClassStorage of
                (Just _) -> return ()
                _        -> createSystemTables systemTables

createSystemTables :: (StorageBackend m) => [TableSchema] -> Kyuu m ()
createSystemTables tables = do
        startTransaction

        forM_ tables $ \(TableSchema tableId _ _) ->
                S.createTable catalogDatabaseId tableId

        pgClassTable     <- fromJust <$> openTable pgClassTableId
        pgAttributeTable <- fromJust <$> openTable pgAttributeTableId

        forM_ tables $ \(TableSchema tableId tableName tableCols) -> do
                let tuple = Tuple [] [VInt tableId, VString tableName]
                insertTuple pgClassTable tuple

                forM_ tableCols $ \(ColumnSchema _ colId colName colType) -> do
                        let
                                tuple = Tuple
                                        []
                                        [ VInt tableId
                                        , VString colName
                                        , VInt colId
                                        ]
                        insertTuple pgAttributeTable tuple

        finishTransaction
