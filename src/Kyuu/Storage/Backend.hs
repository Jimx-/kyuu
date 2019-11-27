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
    type TransactionType m :: *
    createTable :: OID -> OID -> m (TableType m)
    openTable :: OID -> m (Maybe (TableType m))
    startTransaction :: m (TransactionType m)
    commitTransaction :: TransactionType m -> m ()
    insertTuple :: TransactionType m -> TableType m -> B.ByteString -> m ()
    beginTableScan :: TableType m -> m (TableScanIteratorType m)
    tableScanNext :: TableScanIteratorType m -> ScanDirection -> m (TableScanIteratorType m, Maybe (TupleType m))
    getTupleData :: TupleType m -> m B.ByteString

instance (StorageBackend m) => StorageBackend (StateT s m) where
        type TableType (StateT s m) = TableType m
        type TableScanIteratorType (StateT s m) = TableScanIteratorType m
        type TupleType (StateT s m) = TupleType m
        type TransactionType (StateT s m) = TransactionType m
        createTable dbId tableId = lift $ createTable dbId tableId
        openTable         = lift . openTable
        startTransaction  = lift startTransaction
        commitTransaction = lift . commitTransaction
        insertTuple txn table tuple = lift $ insertTuple txn table tuple
        beginTableScan = lift . beginTableScan
        tableScanNext iterator dir = lift $ tableScanNext iterator dir
        getTupleData = lift . getTupleData

instance (StorageBackend m) => StorageBackend (ExceptT e m) where
        type TableType (ExceptT e m) = TableType m
        type TableScanIteratorType (ExceptT e m) = TableScanIteratorType m
        type TupleType (ExceptT e m) = TupleType m
        type TransactionType (ExceptT e m) = TransactionType m
        createTable dbId tableId = lift $ createTable dbId tableId
        openTable         = lift . openTable
        startTransaction  = lift startTransaction
        commitTransaction = lift . commitTransaction
        insertTuple txn table tuple = lift $ insertTuple txn table tuple
        beginTableScan = lift . beginTableScan
        tableScanNext iterator dir = lift $ tableScanNext iterator dir
        getTupleData = lift . getTupleData
