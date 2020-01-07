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
    type TupleSlotType m:: *
    type IndexType m :: *
    type IndexScanIteratorType m :: *
    type TransactionType m :: *

    createTable :: OID -> OID -> m (TableType m)
    openTable :: OID -> OID -> m (Maybe (TableType m))
    createIndex :: OID -> OID -> (B.ByteString -> B.ByteString -> Maybe Ordering) -> m (IndexType m)
    openIndex :: OID -> OID -> (B.ByteString -> B.ByteString -> Maybe Ordering) -> m (Maybe (IndexType m))
    startTransaction :: IsolationLevel -> m (TransactionType m)
    commitTransaction :: TransactionType m -> m ()
    insertTuple :: TransactionType m -> TableType m -> B.ByteString -> m (TupleSlotType m)
    beginTableScan :: TransactionType m -> TableType m -> m (TableScanIteratorType m)
    tableScanNext :: TableScanIteratorType m -> ScanDirection -> m (TableScanIteratorType m, Maybe (TupleType m))
    getTupleData :: TupleType m -> m B.ByteString
    createCheckpoint :: m ()
    getNextOid :: m OID
    insertIndex :: IndexType m -> B.ByteString -> TupleSlotType m -> m ()
    beginIndexScan :: TransactionType m -> IndexType m -> TableType m -> m (IndexScanIteratorType m)
    rescanIndex :: IndexScanIteratorType m -> Maybe B.ByteString -> (B.ByteString -> Maybe Bool)-> m (IndexScanIteratorType m)
    indexScanNext :: IndexScanIteratorType m -> ScanDirection -> m (IndexScanIteratorType m, Maybe (TupleType m))
    endIndexScan :: IndexScanIteratorType m -> m (IndexScanIteratorType m)
    closeIndex :: IndexType m -> m ()


instance StorageBackend m => StorageBackend (StateT s m) where
        type TableType (StateT s m) = TableType m
        type TableScanIteratorType (StateT s m) = TableScanIteratorType m
        type TupleType (StateT s m) = TupleType m
        type TupleSlotType (StateT s m) = TupleSlotType m
        type IndexType (StateT s m) = IndexType m
        type IndexScanIteratorType (StateT s m) = IndexScanIteratorType m
        type TransactionType (StateT s m) = TransactionType m
        createTable dbId tableId = lift $ createTable dbId tableId
        openTable dbId tableId = lift $ openTable dbId tableId
        createIndex dbId indexId keyComp =
                lift $ createIndex dbId indexId keyComp
        openIndex dbId indexId keyComp = lift $ openIndex dbId indexId keyComp
        startTransaction  = lift . startTransaction
        commitTransaction = lift . commitTransaction
        insertTuple txn table tuple = lift $ insertTuple txn table tuple
        beginTableScan txn table = lift $ beginTableScan txn table
        tableScanNext iterator dir = lift $ tableScanNext iterator dir
        getTupleData     = lift . getTupleData
        createCheckpoint = lift createCheckpoint
        getNextOid       = lift getNextOid
        insertIndex index key tupleSlot =
                lift $ insertIndex index key tupleSlot
        beginIndexScan txn index table = lift $ beginIndexScan txn index table
        rescanIndex iterator startKey predicate =
                lift $ rescanIndex iterator startKey predicate
        endIndexScan = lift . endIndexScan
        indexScanNext iterator dir = lift $ indexScanNext iterator dir
        closeIndex = lift . closeIndex

instance StorageBackend m => StorageBackend (ExceptT e m) where
        type TableType (ExceptT e m) = TableType m
        type TableScanIteratorType (ExceptT e m) = TableScanIteratorType m
        type TupleType (ExceptT e m) = TupleType m
        type TupleSlotType (ExceptT e m) = TupleSlotType m
        type IndexType (ExceptT e m) = IndexType m
        type IndexScanIteratorType (ExceptT e m) = IndexScanIteratorType m
        type TransactionType (ExceptT e m) = TransactionType m
        createTable dbId tableId = lift $ createTable dbId tableId
        openTable dbId tableId = lift $ openTable dbId tableId
        createIndex dbId indexId keyComp =
                lift $ createIndex dbId indexId keyComp
        openIndex dbId indexId keyComp = lift $ openIndex dbId indexId keyComp
        startTransaction  = lift . startTransaction
        commitTransaction = lift . commitTransaction
        insertTuple txn table tuple = lift $ insertTuple txn table tuple
        beginTableScan txn table = lift $ beginTableScan txn table
        tableScanNext iterator dir = lift $ tableScanNext iterator dir
        getTupleData     = lift . getTupleData
        createCheckpoint = lift createCheckpoint
        getNextOid       = lift getNextOid
        insertIndex index key tupleSlot =
                lift $ insertIndex index key tupleSlot
        beginIndexScan txn index table = lift $ beginIndexScan txn index table
        rescanIndex iterator startKey predicate =
                lift $ rescanIndex iterator startKey predicate
        endIndexScan = lift . endIndexScan
        indexScanNext iterator dir = lift $ indexScanNext iterator dir
        closeIndex = lift . closeIndex

instance StorageBackend m => StorageBackend (ReaderT r m) where
        type TableType (ReaderT r m) = TableType m
        type TableScanIteratorType (ReaderT r m) = TableScanIteratorType m
        type TupleType (ReaderT r m) = TupleType m
        type TupleSlotType (ReaderT r m) = TupleSlotType m
        type IndexType (ReaderT r m) = IndexType m
        type IndexScanIteratorType (ReaderT r m) = IndexScanIteratorType m
        type TransactionType (ReaderT r m) = TransactionType m
        createTable dbId tableId = lift $ createTable dbId tableId
        openTable dbId tableId = lift $ openTable dbId tableId
        createIndex dbId indexId keyComp =
                lift $ createIndex dbId indexId keyComp
        openIndex dbId indexId keyComp = lift $ openIndex dbId indexId keyComp
        startTransaction  = lift . startTransaction
        commitTransaction = lift . commitTransaction
        insertTuple txn table tuple = lift $ insertTuple txn table tuple
        beginTableScan txn table = lift $ beginTableScan txn table
        tableScanNext iterator dir = lift $ tableScanNext iterator dir
        getTupleData     = lift . getTupleData
        createCheckpoint = lift createCheckpoint
        getNextOid       = lift getNextOid
        insertIndex index key tupleSlot =
                lift $ insertIndex index key tupleSlot
        beginIndexScan txn index table = lift $ beginIndexScan txn index table
        rescanIndex iterator startKey predicate =
                lift $ rescanIndex iterator startKey predicate
        endIndexScan = lift . endIndexScan
        indexScanNext iterator dir = lift $ indexScanNext iterator dir
        closeIndex = lift . closeIndex
