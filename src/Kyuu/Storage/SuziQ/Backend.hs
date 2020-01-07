{-# LANGUAGE FlexibleInstances, TypeFamilies #-}
module Kyuu.Storage.SuziQ.Backend
        ( sqInit
        , sqCreateDB
        , runSuziQ
        , runSuziQWithDB
        )
where

import           Kyuu.Prelude
import           Kyuu.Storage.Backend
import           Kyuu.Storage.SuziQ.FFI
import           Kyuu.Storage.SuziQ.Core
import           Kyuu.Storage.SuziQ.Error

import           Control.Monad.Trans.State.Lazy
import           Control.Monad.Trans.Except
import           Control.Monad.Trans.Identity
import           Control.Monad.Trans.Class
import           Control.Monad.Trans.Control

import qualified Data.ByteString               as B
import qualified Data.ByteString.Char8         as C
import           Data.Char                      ( chr )

import           Foreign.Ptr
import           Foreign.ForeignPtr
import           Foreign.C.Types                ( CChar )
import           Foreign.C.String               ( CString(..)
                                                , newCString
                                                , withCString
                                                )
import           Foreign.Marshal.Alloc

instance StorageBackend SuziQ where
        type TableType SuziQ = SqTable
        type TableScanIteratorType SuziQ = SqTableScanIterator
        type TupleType SuziQ = SqTuple
        type TupleSlotType SuziQ = SqTupleSlot
        type IndexType SuziQ = SqIndex
        type IndexScanIteratorType SuziQ = SqIndexScanIterator
        type TransactionType SuziQ = SqTransaction
        createTable dbId tableId = do
                db <- getDB
                sqCreateTable db dbId tableId

        openTable dbId tableId = do
                db <- getDB
                sqOpenTable db dbId tableId

        createIndex dbId indexId keyComp = do
                db <- getDB
                sqCreateIndex db dbId indexId keyComp

        openIndex dbId indexId keyComp = do
                db <- getDB
                sqOpenIndex db dbId indexId keyComp

        startTransaction isolationLevel = do
                db <- getDB
                sqStartTransaction db isolationLevel

        commitTransaction txn = do
                db <- getDB
                sqCommitTransaction db txn

        insertTuple txn table tuple = do
                db <- getDB
                sqInsertTuple table db txn tuple

        beginTableScan txn table = do
                db <- getDB
                sqBeginTableScan table db txn

        tableScanNext iterator dir = do
                db    <- getDB
                tuple <- sqTableScanNext iterator db dir
                return (iterator, tuple)

        getTupleData     = sqTupleGetData

        createCheckpoint = do
                db <- getDB
                sqCreateCheckpoint db

        getNextOid = do
                db <- getDB
                sqGetNextOid db

        insertIndex index key tupleSlot = do
                db <- getDB
                sqInsertIndex db index key tupleSlot

        beginIndexScan txn index table = do
                db <- getDB
                sqBeginIndexScan index db txn table

        rescanIndex iterator startKey predicate = do
                db <- getDB
                sqRescanIndex db iterator startKey predicate

        indexScanNext iterator dir = do
                db    <- getDB
                tuple <- sqIndexScanNext iterator db dir
                return (iterator, tuple)

        endIndexScan si@(SqIndexScanIterator predicate iterator) =
                case predicate of
                        (Just predicate) -> do
                                liftIO $ freeHaskellFunPtr predicate
                                return (SqIndexScanIterator Nothing iterator)
                        _ -> return si

        closeIndex (SqIndex keyComp _) = case keyComp of
                (Just keyComp) -> liftIO $ freeHaskellFunPtr keyComp
                _              -> return ()


sqInit :: IO ()
sqInit = sq_init

runSuziQ :: String -> SuziQ () -> IO ()
runSuziQ rootPath prog = do
        db <- sqCreateDB rootPath
        case db of
                (Just db) -> runSuziQWithDB db prog
                _         -> putStrLn "cannot create database instance"

runSuziQWithDB :: SqDB -> SuziQ () -> IO ()
runSuziQWithDB db prog = do
        let initState = initSqState db
        res <- runExceptT $ runStateT (unSuziQ prog) initState
        case res of
                Left  err -> putStrLn $ "Uncaught storage error: " ++ show err
                Right _   -> return ()

lastErrorMessage :: (MonadIO m) => m String
lastErrorMessage = do
        len <- liftIO sq_last_error_length
        msg <- liftIO $ allocaBytes (fromIntegral len) $ \ptr -> do
                len <- sq_last_error_message ptr (fromIntegral len)
                C.packCStringLen (ptr, fromIntegral len)
        return $ C.unpack msg

lastError :: (MonadIO m) => m SqErr
lastError = Msg <$> lastErrorMessage

tryGetLastError :: (MonadIO m) => m (Maybe SqErr)
tryGetLastError = do
        msg <- lastErrorMessage
        if null msg then return Nothing else return $ Just (Msg msg)

sqCreateDB :: (MonadIO m) => String -> m (Maybe SqDB)
sqCreateDB rootPath = liftIO $ withCString rootPath $ \cstr -> do
        ptr <- sq_create_db cstr
        if ptr /= nullPtr
                then do
                        foreignPtr <- newForeignPtr sq_free_db ptr
                        return $ Just foreignPtr
                else do
                        err <- lastError
                        return Nothing

sqCreateTable :: SqDB -> OID -> OID -> SuziQ SqTable
sqCreateTable db dbId tableId = control $ \runInBase ->
        withForeignPtr db $ \database -> do
                ptr <- sq_create_table database
                                       (fromIntegral dbId)
                                       (fromIntegral tableId)
                runInBase $ if ptr /= nullPtr
                        then liftIO $ newForeignPtr sq_free_table ptr
                        else do
                                err <- lastError
                                lerror err


sqOpenTable :: SqDB -> OID -> OID -> SuziQ (Maybe SqTable)
sqOpenTable db dbId tableId = control $ \runInBase ->
        withForeignPtr db $ \database -> do
                ptr <- sq_open_table database
                                     (fromIntegral dbId)
                                     (fromIntegral tableId)
                runInBase $ if ptr /= nullPtr
                        then do
                                foreignPtr <- liftIO
                                        $ newForeignPtr sq_free_table ptr
                                return $ Just foreignPtr
                        else do
                                err <- tryGetLastError
                                case err of
                                        (Just err) -> lerror err
                                        _          -> return Nothing

sqCreateIndex
        :: SqDB
        -> OID
        -> OID
        -> (B.ByteString -> B.ByteString -> Maybe Ordering)
        -> SuziQ SqIndex
sqCreateIndex db dbId indexId keyComp = control $ \runInBase ->
        withForeignPtr db $ \database -> do
                keyCompFunc <- sqWrapRawIndexKeyComparator
                        $ getIndexKeyComparator keyComp
                ptr <- sq_create_index database
                                       (fromIntegral dbId)
                                       (fromIntegral indexId)
                                       keyCompFunc
                runInBase $ if ptr /= nullPtr
                        then do
                                foreignPtr <- liftIO
                                        $ newForeignPtr sq_free_index ptr
                                return $ SqIndex (Just keyCompFunc) foreignPtr
                        else do
                                err <- lastError
                                lerror err


sqOpenIndex
        :: SqDB
        -> OID
        -> OID
        -> (B.ByteString -> B.ByteString -> Maybe Ordering)
        -> SuziQ (Maybe SqIndex)
sqOpenIndex db dbId indexId keyComp = control $ \runInBase ->
        withForeignPtr db $ \database -> do
                keyCompFunc <- sqWrapRawIndexKeyComparator
                        $ getIndexKeyComparator keyComp
                ptr <- sq_open_index database
                                     (fromIntegral dbId)
                                     (fromIntegral indexId)
                                     keyCompFunc

                runInBase $ if ptr /= nullPtr
                        then do
                                foreignPtr <- liftIO
                                        $ newForeignPtr sq_free_index ptr
                                return $ Just $ SqIndex
                                        (Just keyCompFunc)
                                        foreignPtr
                        else do
                                err <- tryGetLastError
                                case err of
                                        (Just err) -> lerror err
                                        _          -> return Nothing

getIsolationLevel :: IsolationLevel -> Int
getIsolationLevel ReadUncommitted = 0
getIsolationLevel ReadCommitted   = 1
getIsolationLevel RepeatableRead  = 2
getIsolationLevel Serializable    = 3

sqStartTransaction :: SqDB -> IsolationLevel -> SuziQ SqTransaction
sqStartTransaction db isoLevel = do
        txn <- liftIO $ withForeignPtr db $ \database -> do
                ptr <- sq_start_transaction
                        database
                        (fromIntegral $ getIsolationLevel isoLevel)
                if ptr /= nullPtr
                        then do
                                foreignPtr <- newForeignPtr
                                        sq_free_transaction
                                        ptr
                                return $ Just foreignPtr
                        else return Nothing

        case txn of
                (Just txn) -> return txn
                _          -> do
                        err <- lastError
                        lerror err

sqCommitTransaction :: SqDB -> SqTransaction -> SuziQ ()
sqCommitTransaction db txn = do
        result <- liftIO $ withForeignPtr db $ \database ->
                withForeignPtr txn $ \txn -> sq_commit_transaction database txn

        error <- tryGetLastError
        case error of
                Nothing    -> return ()
                (Just err) -> lerror err

sqInsertTuple
        :: SqTable -> SqDB -> SqTransaction -> B.ByteString -> SuziQ SqTupleSlot
sqInsertTuple table db txn tuple = control $ \runInBase -> do
        withForeignPtr db $ \database -> withForeignPtr table $ \table ->
                withForeignPtr txn $ \txn ->
                        B.useAsCStringLen tuple $ \(buf, len) -> do
                                ptr <- sq_table_insert_tuple
                                        table
                                        database
                                        txn
                                        buf
                                        (fromIntegral len)
                                runInBase $ if ptr /= nullPtr
                                        then liftIO $ newForeignPtr
                                                sq_free_item_pointer
                                                ptr
                                        else do
                                                err <- lastError
                                                lerror err


sqBeginTableScan
        :: SqTable -> SqDB -> SqTransaction -> SuziQ SqTableScanIterator
sqBeginTableScan table db txn = do
        iterator <-
                liftIO
                $ withForeignPtr db
                $ \database -> withForeignPtr table $ \table ->
                          withForeignPtr txn $ \txn -> do
                                  ptr <- sq_table_begin_scan table database txn

                                  if ptr /= nullPtr
                                          then do
                                                  foreignPtr <- newForeignPtr
                                                          sq_free_table_scan_iterator
                                                          ptr
                                                  return $ Just foreignPtr
                                          else return Nothing

        case iterator of
                (Just iterator) -> return iterator
                _               -> do
                        err <- lastError
                        lerror err

getScanDirection :: ScanDirection -> Int
getScanDirection Forward  = 0
getScanDirection Backward = 1

sqTableScanNext
        :: SqTableScanIterator -> SqDB -> ScanDirection -> SuziQ (Maybe SqTuple)
sqTableScanNext iterator db dir = control $ \runInBase ->
        withForeignPtr db $ \database -> withForeignPtr iterator $ \iterator ->
                do
                        ptr <- sq_table_scan_next
                                iterator
                                database
                                (fromIntegral $ getScanDirection dir)

                        runInBase $ if ptr /= nullPtr
                                then do
                                        foreignPtr <- liftIO $ newForeignPtr
                                                sq_free_tuple
                                                ptr
                                        return $ Just foreignPtr
                                else do
                                        err <- tryGetLastError
                                        case err of
                                                (Just err) -> lerror err
                                                _          -> return Nothing


sqTupleGetData :: SqTuple -> SuziQ B.ByteString
sqTupleGetData tuple = liftIO $ withForeignPtr tuple $ \tuple -> do
        len <- sq_tuple_get_data_len tuple
        liftIO $ allocaBytes (fromIntegral len) $ \ptr -> do
                len <- sq_tuple_get_data tuple ptr len
                B.packCStringLen (ptr, fromIntegral len)

sqCreateCheckpoint :: SqDB -> SuziQ ()
sqCreateCheckpoint db = control $ \runInBase ->
        withForeignPtr db $ \database -> do
                sq_create_checkpoint database
                err <- tryGetLastError
                runInBase $ case err of
                        Nothing    -> return ()
                        (Just err) -> lerror err

sqGetNextOid :: SqDB -> SuziQ OID
sqGetNextOid db = control $ \runInBase -> withForeignPtr db $ \database -> do
        oid <- sq_get_next_oid database
        err <- tryGetLastError
        runInBase $ case err of
                Nothing    -> return (fromIntegral oid)
                (Just err) -> lerror err

getOrdering :: Ordering -> Int
getOrdering LT = -1
getOrdering EQ = 0
getOrdering GT = 1

getIndexKeyComparator
        :: (B.ByteString -> B.ByteString -> Maybe Ordering)
        -> RawIndexKeyComparatorFunc
getIndexKeyComparator f aPtr aLen bPtr bLen = do
        aStr <- B.packCStringLen (aPtr, fromIntegral aLen)
        bStr <- B.packCStringLen (bPtr, fromIntegral bLen)

        case f aStr bStr of
                (Just ord) -> return $ fromIntegral $ getOrdering ord
                _          -> return 2


sqInsertIndex :: SqDB -> SqIndex -> B.ByteString -> SqTupleSlot -> SuziQ ()
sqInsertIndex db (SqIndex _ indexPtr) key slot = control $ \runInBase ->
        withForeignPtr db $ \database -> withForeignPtr indexPtr $ \indexPtr ->
                withForeignPtr slot $ \slot ->
                        B.useAsCStringLen key $ \(keyPtr, keyLen) -> do
                                sq_index_insert indexPtr
                                                database
                                                keyPtr
                                                (fromIntegral keyLen)
                                                slot
                                err <- tryGetLastError
                                runInBase $ case err of
                                        Nothing    -> return ()
                                        (Just err) -> lerror err

sqBeginIndexScan
        :: SqIndex
        -> SqDB
        -> SqTransaction
        -> SqTable
        -> SuziQ SqIndexScanIterator
sqBeginIndexScan (SqIndex _ indexPtr) db txn table = control $ \runInBase ->
        withForeignPtr db $ \database ->
                withForeignPtr indexPtr
                        $ \indexPtr -> withForeignPtr txn $ \txn ->
                                  withForeignPtr table $ \table -> do
                                          ptr <- sq_index_begin_scan
                                                  indexPtr
                                                  database
                                                  txn
                                                  table

                                          runInBase $ if ptr /= nullPtr
                                                  then do
                                                          foreignPtr <-
                                                                  liftIO
                                                                          $ newForeignPtr
                                                                                    sq_free_index_scan_iterator
                                                                                    ptr
                                                          return
                                                                  $ SqIndexScanIterator
                                                                            Nothing
                                                                            foreignPtr
                                                  else do
                                                          err <- lastError
                                                          lerror err


getIndexScanPredicate :: (B.ByteString -> Maybe Bool) -> RawIndexScanPredicate
getIndexScanPredicate f aPtr aLen = do
        aStr <- B.packCStringLen (aPtr, fromIntegral aLen)

        case f aStr of
                (Just False) -> return 0
                (Just True ) -> return 1
                _            -> return 2

sqRescanIndex
        :: SqDB
        -> SqIndexScanIterator
        -> Maybe B.ByteString
        -> (B.ByteString -> Maybe Bool)
        -> SuziQ SqIndexScanIterator
sqRescanIndex db (SqIndexScanIterator oldPred iteratorPtr) startKey predicate =
        control $ \runInBase -> withForeignPtr db $ \database ->
                withForeignPtr iteratorPtr $ \iterator -> do
                        case oldPred of
                                (Just oldPred) -> freeHaskellFunPtr oldPred
                                _              -> return ()

                        predicateFunc <- sqWrapRawIndexScanPredicate
                                $ getIndexScanPredicate predicate

                        case startKey of
                                (Just startKey) ->
                                        B.useAsCStringLen startKey
                                                $ \(keyPtr, keyLen) -> do

                                                          sq_index_rescan
                                                                  iterator
                                                                  database
                                                                  keyPtr
                                                                  (fromIntegral
                                                                          keyLen
                                                                  )
                                                                  predicateFunc
                                _ -> sq_index_rescan iterator
                                                     database
                                                     nullPtr
                                                     0
                                                     predicateFunc
                        err <- tryGetLastError
                        runInBase $ case err of
                                Nothing ->
                                        return
                                                (SqIndexScanIterator
                                                        (Just predicateFunc)
                                                        iteratorPtr
                                                )
                                (Just err) -> lerror err


sqIndexScanNext
        :: SqIndexScanIterator -> SqDB -> ScanDirection -> SuziQ (Maybe SqTuple)
sqIndexScanNext (SqIndexScanIterator predicate iteratorPtr) db dir =
        control $ \runInBase -> withForeignPtr db $ \database ->
                withForeignPtr iteratorPtr $ \iterator -> do
                        ptr <- sq_index_scan_next
                                iterator
                                database
                                (fromIntegral $ getScanDirection dir)

                        runInBase $ if ptr /= nullPtr
                                then do
                                        foreignPtr <- liftIO $ newForeignPtr
                                                sq_free_tuple
                                                ptr
                                        return $ Just foreignPtr
                                else do
                                        err <- tryGetLastError
                                        case err of
                                                (Just err) -> lerror err
                                                _          -> return Nothing
