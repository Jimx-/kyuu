{-# LANGUAGE TypeFamilies, FlexibleContexts #-}
module Kyuu.Storage.Backend
        ( StorageBackend(..)
        , ScanDirection(..)
        , IsolationLevel(..)
        )
where

import           Kyuu.Prelude

import           Control.Monad.Trans.State.Lazy
import           Control.Monad.Trans.Except
import           Control.Monad.Trans.Reader
import           Control.Monad.Trans.Class
import           Control.Monad.Trans.Control

import qualified Data.ByteString               as B

data ScanDirection = Forward | Backward

data IsolationLevel = ReadUncommitted
                    | ReadCommitted
                    | RepeatableRead
                    | Serializable

class (MonadBaseControl IO m, MonadIO m) => StorageBackend m where
    type TableType m :: *
    type TableScanIteratorType m :: *
    type TupleType m:: *
    type TransactionType m :: *

    createTable :: OID -> OID -> m (TableType m)
    openTable :: OID -> OID -> m (Maybe (TableType m))
    startTransaction :: IsolationLevel -> m (TransactionType m)
    commitTransaction :: TransactionType m -> m ()
    insertTuple :: TransactionType m -> TableType m -> B.ByteString -> m ()
    beginTableScan :: TransactionType m -> TableType m -> m (TableScanIteratorType m)
    tableScanNext :: TableScanIteratorType m -> ScanDirection -> m (TableScanIteratorType m, Maybe (TupleType m))
    getTupleData :: TupleType m -> m B.ByteString
    createCheckpoint :: m ()
    getNextOid :: m OID


instance (StorageBackend m) => StorageBackend (StateT s m) where
        type TableType (StateT s m) = TableType m
        type TableScanIteratorType (StateT s m) = TableScanIteratorType m
        type TupleType (StateT s m) = TupleType m
        type TransactionType (StateT s m) = TransactionType m
        createTable dbId tableId = lift $ createTable dbId tableId
        openTable dbId tableId = lift $ openTable dbId tableId
        startTransaction  = lift . startTransaction
        commitTransaction = lift . commitTransaction
        insertTuple txn table tuple = lift $ insertTuple txn table tuple
        beginTableScan txn table = lift $ beginTableScan txn table
        tableScanNext iterator dir = lift $ tableScanNext iterator dir
        getTupleData     = lift . getTupleData
        createCheckpoint = lift createCheckpoint
        getNextOid       = lift getNextOid

instance (StorageBackend m) => StorageBackend (ExceptT e m) where
        type TableType (ExceptT e m) = TableType m
        type TableScanIteratorType (ExceptT e m) = TableScanIteratorType m
        type TupleType (ExceptT e m) = TupleType m
        type TransactionType (ExceptT e m) = TransactionType m
        createTable dbId tableId = lift $ createTable dbId tableId
        openTable dbId tableId = lift $ openTable dbId tableId
        startTransaction  = lift . startTransaction
        commitTransaction = lift . commitTransaction
        insertTuple txn table tuple = lift $ insertTuple txn table tuple
        beginTableScan txn table = lift $ beginTableScan txn table
        tableScanNext iterator dir = lift $ tableScanNext iterator dir
        getTupleData     = lift . getTupleData
        createCheckpoint = lift createCheckpoint
        getNextOid       = lift getNextOid

instance (StorageBackend m) => StorageBackend (ReaderT r m) where
        type TableType (ReaderT r m) = TableType m
        type TableScanIteratorType (ReaderT r m) = TableScanIteratorType m
        type TupleType (ReaderT r m) = TupleType m
        type TransactionType (ReaderT r m) = TransactionType m
        createTable dbId tableId = lift $ createTable dbId tableId
        openTable dbId tableId = lift $ openTable dbId tableId
        startTransaction  = lift . startTransaction
        commitTransaction = lift . commitTransaction
        insertTuple txn table tuple = lift $ insertTuple txn table tuple
        beginTableScan txn table = lift $ beginTableScan txn table
        tableScanNext iterator dir = lift $ tableScanNext iterator dir
        getTupleData     = lift . getTupleData
        createCheckpoint = lift createCheckpoint
        getNextOid       = lift getNextOid
