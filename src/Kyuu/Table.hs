{-# LANGUAGE NamedFieldPuns #-}

module Kyuu.Table
  ( TupleSlot,
    Table (..),
    TableScanIterator,
    insertTuple,
    beginTableScan,
    tableScanNext,
    estimateTableSize,
    module X,
  )
where

import Kyuu.Catalog.Schema
import Kyuu.Core
import Kyuu.Error
import Kyuu.Prelude
import Kyuu.Storage.Backend as X
  ( ScanDirection (..),
  )
import qualified Kyuu.Storage.Backend as S
import Kyuu.Value

type TupleSlot m = S.TupleSlotType m

data Table m = Table
  { tableId :: OID,
    tableSchema :: TableSchema,
    tableStorage :: S.TableType m
  }

data TableScanIterator m = TableScanIterator
  { table :: Table m,
    iterator :: S.TableScanIteratorType m
  }

insertTuple :: (StorageBackend m) => Table m -> Tuple -> Kyuu m (TupleSlot m)
insertTuple Table {tableStorage} tuple = do
  txn <- getCurrentTransaction
  let tupleBuf = encodeTuple tuple
  S.insertTuple txn tableStorage tupleBuf

beginTableScan :: (StorageBackend m) => Table m -> Kyuu m (TableScanIterator m)
beginTableScan table@Table {tableStorage} = do
  txn <- getCurrentTransaction
  iterator <- S.beginTableScan txn tableStorage
  return $ TableScanIterator table iterator

tableScanNext ::
  (StorageBackend m) =>
  TableScanIterator m ->
  ScanDirection ->
  Kyuu m (TableScanIterator m, Maybe Tuple)
tableScanNext TableScanIterator {table = table@Table {tableSchema}, iterator} dir =
  do
    (newIterator, tableTuple) <- S.tableScanNext iterator dir
    let newIt = TableScanIterator table newIterator
    case tableTuple of
      Nothing -> return (newIt, Nothing)
      (Just tableTuple) -> do
        tupleBuf <- S.getTupleData tableTuple
        case decodeTuple tupleBuf tableSchema of
          (Just tuple) ->
            return (newIt, Just tuple)
          Nothing ->
            lerror
              ( DataCorrupted
                  "cannot decode tuple"
              )

estimateTableSize :: (StorageBackend m) => Table m -> Kyuu m Int
estimateTableSize Table {tableStorage} = do
  fileSize <- S.tableFileSize tableStorage
  return fileSize
