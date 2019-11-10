{-# LANGUAGE TypeFamilies #-}
module Kyuu.Storage.Backend
        ( StorageBackend(..)
        , ScanDirection(..)
        )
where

import           Kyuu.Prelude

import           Control.Monad.Trans.State.Lazy
import           Control.Monad.Trans.Except
import           Control.Monad.Trans.Class

import qualified Data.ByteString               as B

data ScanDirection = Forward | Backward

class MonadIO m => StorageBackend m where
    type TableType m :: *
    type TableScanIteratorType m :: *
    type TupleType m:: *
    createTable :: OID -> OID -> m (TableType m)
    insertTuple :: TableType m -> B.ByteString -> m ()
    beginTableScan :: TableType m -> m (TableScanIteratorType m)
    tableScanNext :: TableScanIteratorType m -> ScanDirection -> m (Maybe (TupleType m))
    getTupleData :: TupleType m -> m B.ByteString

instance (StorageBackend m) => StorageBackend (StateT s m) where
        type TableType (StateT s m) = TableType m
        type TableScanIteratorType (StateT s m) = TableScanIteratorType m
        type TupleType (StateT s m) = TupleType m
        createTable dbId tableId = lift $ createTable dbId tableId
        insertTuple table tuple = lift $ insertTuple table tuple
        beginTableScan = lift . beginTableScan
        tableScanNext iterator dir = lift $ tableScanNext iterator dir
        getTupleData = lift . getTupleData

instance (StorageBackend m) => StorageBackend (ExceptT e m) where
        type TableType (ExceptT e m) = TableType m
        type TableScanIteratorType (ExceptT e m) = TableScanIteratorType m
        type TupleType (ExceptT e m) = TupleType m
        createTable dbId tableId = lift $ createTable dbId tableId
        insertTuple table tuple = lift $ insertTuple table tuple
        beginTableScan = lift . beginTableScan
        tableScanNext iterator dir = lift $ tableScanNext iterator dir
        getTupleData = lift . getTupleData
