{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NamedFieldPuns #-}

module Kyuu.Catalog.Catalog
  ( CatalogState (..),
    initCatalogState,
    bootstrapCatalog,
    lookupTableById,
    lookupTableIdByName,
    getTableColumns,
    lookupTableColumnById,
    lookupTableColumnByName,
    createTableWithCatalog,
    openTable,
    openIndex,
    openIndexes,
  )
where

import Control.Lens hiding (Index)
import qualified Data.ByteString as B
import Data.List (find)
import Data.Map ()
import Data.Maybe (fromJust)
import Kyuu.Catalog.Schema
import Kyuu.Catalog.State
import Kyuu.Catalog.Tables
import Kyuu.Core
import Kyuu.Error
import Kyuu.Expression
import Kyuu.Index
import Kyuu.Prelude
import qualified Kyuu.Storage.Backend as S
import Kyuu.Table
import Kyuu.Value

catalogDatabaseId :: OID
catalogDatabaseId = 1

lookupTableById :: (StorageBackend m) => OID -> Kyuu m (Maybe TableSchema)
lookupTableById tId = do
  state <- getCatalogState
  return $
    find
      (\TableSchema {tableId} -> tableId == tId)
      (state ^. tableSchemas_)

getTableById :: (StorageBackend m) => OID -> Kyuu m (Maybe TableSchema)
getTableById tableId = do
  state <- getCatalogState

  case find
    (\TableSchema {tableId = tableId'} -> tableId == tableId')
    (state ^. tableSchemas_) of
    (Just tableSchema) -> do
      return $ Just tableSchema
    _ -> do
      tableSchema <- buildTableSchema tableId
      case tableSchema of
        Nothing -> do
          return Nothing
        (Just tableSchema) -> do
          state <- takeCatalogState
          let newState = over tableSchemas_ (tableSchema :) state
          putCatalogState newState
          return $ Just tableSchema

lookupTableIdByName :: (StorageBackend m) => String -> Kyuu m (Maybe OID)
lookupTableIdByName name = do
  state <- getCatalogState

  case find ((== name) . tableName) (state ^. tableSchemas_) of
    (Just TableSchema {tableId}) -> return $ Just tableId
    _ -> do
      tuple <- scanRelationByName name
      case tuple of
        Nothing -> return Nothing
        (Just tuple) -> do
          relId <- evalExpr (ColumnRefExpr pgClassTableId classOidColNum) tuple

          case relId of
            (VInt relId) -> return $ Just relId
            _ -> lerror (DataCorrupted "invalid type of class tuple")

getTableColumns :: (StorageBackend m) => OID -> Kyuu m [ColumnSchema]
getTableColumns tId = do
  state <- getCatalogState
  let schema =
        fromJust $
          find (\TableSchema {tableId = tableId} -> tableId == tId) (state ^. tableSchemas_)
  return $ tableCols schema

lookupTableColumnById ::
  (StorageBackend m) => OID -> OID -> Kyuu m (Maybe ColumnSchema)
lookupTableColumnById tId cId = do
  table <- lookupTableById tId
  case table of
    Just TableSchema {tableCols = tableCols} ->
      return $ find ((== cId) . colId) tableCols
    _ -> return Nothing

lookupTableColumnByName ::
  (StorageBackend m) => OID -> String -> Kyuu m (Maybe ColumnSchema)
lookupTableColumnByName tId name = do
  table <- lookupTableById tId
  case table of
    Just TableSchema {tableCols = tableCols} ->
      return $ find ((== name) . colName) tableCols
    _ -> return Nothing

createTableWithCatalog :: (StorageBackend m) => TableSchema -> Kyuu m (Table m)
createTableWithCatalog (TableSchema _ tableName tableCols) = do
  exists <- lookupTableIdByName tableName

  case exists of
    Just _ -> lerror (TableExists tableName)
    Nothing -> do
      state <- getCatalogState
      tableId <- getNextOid
      let schema =
            TableSchema
              tableId
              tableName
              (map (\x -> x {colTable = tableId}) tableCols)
      modifyCatalogState $ over tableSchemas_ (schema :)
      storage <- S.createTable 1 tableId

      addNewRelationTuple schema
      addNewAttributeTuples schema

      return $ Table tableId schema storage

addNewRelationTuple :: (StorageBackend m) => TableSchema -> Kyuu m ()
addNewRelationTuple tableSchema = do
  pgClassTable <- fromJust <$> openTable pgClassTableId
  let tuple = getClassTableTuple tableSchema
  catalogInsertTuple pgClassTable tuple

addNewAttributeTuples :: (StorageBackend m) => TableSchema -> Kyuu m ()
addNewAttributeTuples TableSchema {tableId, tableCols} = do
  pgAttributeTable <- fromJust <$> openTable pgAttributeTableId

  forM_ tableCols $ \columnSchema -> do
    let tuple = getAttributeTableTuple tableId columnSchema
    catalogInsertTuple pgAttributeTable tuple

openTable :: (StorageBackend m) => OID -> Kyuu m (Maybe (Table m))
openTable tableId = do
  schema <- getTableById tableId

  case schema of
    Nothing -> return Nothing
    (Just schema) -> do
      storage <- S.openTable 1 tableId
      case storage of
        (Just storage) -> return $ Just (Table tableId schema storage)
        _ ->
          lerror $
            InvalidState ("storage not created for table " ++ show tableId)

lookupIndexById :: (StorageBackend m) => OID -> Kyuu m (Maybe IndexSchema)
lookupIndexById indId = do
  state <- getCatalogState
  return $ find (\IndexSchema {indexId} -> indexId == indId) (state ^. indexSchemas_)

keyComparator :: B.ByteString -> B.ByteString -> Maybe Ordering
keyComparator a b = case decodeTupleWithDesc a [] of
  Nothing -> Nothing
  (Just aTuple) -> case decodeTupleWithDesc b [] of
    Nothing -> Nothing
    (Just bTuple) -> Just $ compareTuple aTuple bTuple

openIndex :: (StorageBackend m) => OID -> Kyuu m (Maybe (Index m))
openIndex indexId = do
  schema <- lookupIndexById indexId

  case schema of
    Nothing -> return Nothing
    (Just schema) -> do
      storage <- S.openIndex 1 indexId keyComparator
      case storage of
        (Just storage) -> return $ Just (Index indexId schema storage)
        _ ->
          lerror $
            InvalidState ("storage not created for index " ++ show indexId)

openIndexes :: (StorageBackend m) => Table m -> Kyuu m [Index m]
openIndexes table = do
  indexOids <- getTableIndexList table
  forM indexOids $ \indexId -> do
    index <- openIndex indexId
    case index of
      Nothing -> lerror $ IndexNotFound indexId
      (Just index) -> return index

bootstrapCatalog :: (StorageBackend m) => Kyuu m ()
bootstrapCatalog = do
  let systemTables =
        [ pgClassTableSchema,
          pgAttributeTableSchema,
          pgIndexTableSchema
        ]
      systemIndexes =
        [ classOidIndexSchema,
          classNameIndexSchema,
          attributeRelIdColNumIndexSchema,
          indexIndRelIdIndexSchema
        ]

  forM_ systemTables $
    \schema -> modifyCatalogState $ over tableSchemas_ (schema :)

  forM_ systemIndexes $
    \schema -> modifyCatalogState $ over indexSchemas_ (schema :)

  pgClassStorage <- S.openTable catalogDatabaseId pgClassTableId
  case pgClassStorage of
    (Just _) -> return ()
    _ -> createSystemTablesAndIndexes systemTables systemIndexes

createSystemTablesAndIndexes ::
  (StorageBackend m) => [TableSchema] -> [IndexSchema] -> Kyuu m ()
createSystemTablesAndIndexes tables indexes = do
  startTransaction

  forM_ tables $
    \(TableSchema tableId _ _) -> S.createTable catalogDatabaseId tableId

  forM_ indexes $ \(IndexSchema indexId _ _) ->
    S.createIndex catalogDatabaseId indexId keyComparator

  pgClassTable <- fromJust <$> openTable pgClassTableId
  pgAttributeTable <- fromJust <$> openTable pgAttributeTableId
  pgIndexTable <- fromJust <$> openTable pgIndexTableId
  classOidIndex <- fromJust <$> openIndex classOidIndexId
  classNameIndex <- fromJust <$> openIndex classNameIndexId
  attributeRelIdColNumIndex <-
    fromJust
      <$> openIndex attributeRelIdColNumIndexId
  indexIndRelIdIndex <- fromJust <$> openIndex indexIndRelIdIndexId

  -- Populate pg_class and pg_attribute with system tables.
  forM_ tables $ \tableSchema@TableSchema {tableId, tableCols} -> do
    let tuple = getClassTableTuple tableSchema
    slot <- insertTuple pgClassTable tuple
    formIndexKeyTuple classOidIndex tuple >>= \keyTuple -> insertIndex classOidIndex keyTuple slot
    formIndexKeyTuple classNameIndex tuple >>= \keyTuple -> insertIndex classNameIndex keyTuple slot

    forM_ tableCols $ \columnSchema -> do
      let tuple = getAttributeTableTuple tableId columnSchema
      slot <- insertTuple pgAttributeTable tuple
      formIndexKeyTuple attributeRelIdColNumIndex tuple >>= \keyTuple -> insertIndex attributeRelIdColNumIndex keyTuple slot

  forM_ indexes $ \indexSchema@(IndexSchema indexId indexTableId colNums) -> do
    let tuple = getIndexTableTuple indexSchema
    slot <- insertTuple pgIndexTable tuple
    formIndexKeyTuple indexIndRelIdIndex tuple >>= \keyTuple -> insertIndex indexIndRelIdIndex keyTuple slot

  finishTransaction

scanRelation :: (StorageBackend m) => OID -> Kyuu m (Maybe Tuple)
scanRelation relId = do
  pgClassTable <- fromJust <$> openTable pgClassTableId
  classOidIndex <- fromJust <$> openIndex classOidIndexId

  si <- beginIndexScan classOidIndex pgClassTable
  si <- rescanIndex si [ScanKey classOidColNum SEqual (VInt relId)] Forward
  (si, tuple) <- indexScanNext si Forward
  si <- endIndexScan si

  return tuple

scanRelationByName :: (StorageBackend m) => String -> Kyuu m (Maybe Tuple)
scanRelationByName relName = do
  pgClassTable <- fromJust <$> openTable pgClassTableId
  classNameIndex <- fromJust <$> openIndex classNameIndexId

  si <- beginIndexScan classNameIndex pgClassTable
  si <- rescanIndex si [ScanKey classNameColNum SEqual (VString relName)] Forward
  (si, tuple) <- indexScanNext si Forward
  si <- endIndexScan si

  return tuple

buildTableSchema :: (StorageBackend m) => OID -> Kyuu m (Maybe TableSchema)
buildTableSchema tableId = do
  tuple <- scanRelation tableId

  case tuple of
    Nothing -> return Nothing
    (Just tuple) -> do
      relId <- evalExpr (ColumnRefExpr pgClassTableId classOidColNum) tuple
      relName <- evalExpr (ColumnRefExpr pgClassTableId classNameColNum) tuple

      case (relId, relName) of
        (VInt relId, VString relName) -> do
          columns <- buildTableColumns tableId
          return $ Just $ TableSchema relId relName columns
        _ -> lerror (DataCorrupted "invalid type of class tuple")

buildTableColumns :: (StorageBackend m) => OID -> Kyuu m [ColumnSchema]
buildTableColumns tableId = do
  pgAttributeTable <- fromJust <$> openTable pgAttributeTableId
  attributeRelIdColNumIndex <-
    fromJust
      <$> openIndex attributeRelIdColNumIndexId

  si <- beginIndexScan attributeRelIdColNumIndex pgAttributeTable
  si <-
    rescanIndex
      si
      [ ScanKey attributeRelIdColNum SEqual (VInt tableId),
        ScanKey attributeColNumColNum SGreater (VInt 0)
      ]
      Forward

  (si, cols) <- collectColumns si
  si <- endIndexScan si

  return cols
  where
    collectColumns ::
      (StorageBackend m) =>
      IndexScanIterator m ->
      Kyuu m (IndexScanIterator m, [ColumnSchema])
    collectColumns si = do
      (si, tuple) <- indexScanNext si Forward

      case tuple of
        Nothing -> return (si, [])
        (Just tuple) -> do
          relId <-
            evalExpr
              (ColumnRefExpr pgAttributeTableId attributeRelIdColNum)
              tuple
          colName <-
            evalExpr
              (ColumnRefExpr pgAttributeTableId attributeNameColNum)
              tuple
          colType <-
            evalExpr
              (ColumnRefExpr pgAttributeTableId attributeTypeColNum)
              tuple
          colNum <-
            evalExpr
              (ColumnRefExpr pgAttributeTableId attributeColNumColNum)
              tuple

          case (relId, colName, colType, colNum) of
            (VInt relId, VString colName, VInt colType, VInt colNum) -> do
              (si, rest) <- collectColumns si
              return
                (si, ColumnSchema relId colNum colName (toEnum colType) : rest)
            _ -> lerror (DataCorrupted "invalid type of attribute tuple")

getTableIndexList :: (StorageBackend m) => Table m -> Kyuu m [OID]
getTableIndexList Table {tableId} = do
  pgIndexTable <- fromJust <$> openTable pgIndexTableId
  indexIndRelIdIndex <-
    fromJust
      <$> openIndex indexIndRelIdIndexId

  si <- beginIndexScan indexIndRelIdIndex pgIndexTable
  si <-
    rescanIndex
      si
      [ScanKey indexIndRelIdColNum SEqual (VInt tableId)]
      Forward

  (si, indexes) <- collectIndexes si
  si <- endIndexScan si

  return indexes
  where
    collectIndexes ::
      (StorageBackend m) =>
      IndexScanIterator m ->
      Kyuu
        m
        (IndexScanIterator m, [OID])
    collectIndexes si = do
      (si, tuple) <- indexScanNext si Forward

      case tuple of
        Nothing -> return (si, [])
        (Just tuple) -> do
          relId <-
            evalExpr
              (ColumnRefExpr pgIndexTableId indexRelIdColNum)
              tuple

          case relId of
            (VInt relId) -> do
              (si, rest) <- collectIndexes si
              return
                (si, relId : rest)
            _ -> lerror (DataCorrupted "invalid type of index tuple")

catalogInsertTuple :: (StorageBackend m) => Table m -> Tuple -> Kyuu m ()
catalogInsertTuple table tuple = do
  indexes <- openIndexes table
  slot <- insertTuple table tuple

  forM_ indexes $ \index -> do
    keyTuple <- formIndexKeyTuple index tuple
    insertIndex index keyTuple slot
