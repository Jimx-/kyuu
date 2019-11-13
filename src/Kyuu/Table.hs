{-# LANGUAGE NamedFieldPuns #-}
module Kyuu.Table
        ( Table(..)
        , TableScanIterator
        , insertTuple
        , beginTableScan
        , tableScanNext
        , module X
        )
where

import           Kyuu.Prelude
import           Kyuu.Core
import           Kyuu.Value
import           Kyuu.Error
import           Kyuu.Catalog.Schema
import qualified Kyuu.Storage.Backend          as S
import           Kyuu.Storage.Backend          as X
                                                ( ScanDirection(..) )

data Table m = Table { tableId :: OID
                     , tableSchema :: TableSchema
                     , tableStorage :: S.TableType m}

data TableScanIterator m = TableScanIterator { table :: Table m
                                             , iterator :: S.TableScanIteratorType m }

insertTuple :: (StorageBackend m) => Table m -> Tuple -> Kyuu m ()
insertTuple Table { tableStorage } tuple = do
        let tupleBuf = encodeTuple tuple
        S.insertTuple tableStorage tupleBuf

beginTableScan :: (StorageBackend m) => Table m -> Kyuu m (TableScanIterator m)
beginTableScan table@Table { tableStorage } = do
        iterator <- S.beginTableScan tableStorage
        return $ TableScanIterator table iterator

tableScanNext
        :: (StorageBackend m)
        => TableScanIterator m
        -> ScanDirection
        -> Kyuu m (TableScanIterator m, Maybe Tuple)
tableScanNext TableScanIterator { table = table@Table { tableSchema }, iterator } dir
        = do
                (newIterator, tableTuple) <- S.tableScanNext iterator dir
                let newIt = TableScanIterator table newIterator
                case tableTuple of
                        Nothing           -> return (newIt, Nothing)
                        (Just tableTuple) -> do
                                tupleBuf <- S.getTupleData tableTuple
                                case decodeTuple tupleBuf tableSchema of
                                        (Just tuple) ->
                                                return (newIt, Just tuple)
                                        Nothing ->
                                                lerror
                                                        (DataCorrupted
                                                                "cannot decode tuple"
                                                        )
