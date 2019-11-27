{-# LANGUAGE FlexibleInstances, TypeFamilies #-}
module Kyuu.Storage.SuziQ.Backend
        ( runSuziQ
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
        type TransactionType SuziQ = SqTransaction
        createTable dbId tableId = do
                db <- getDB
                sqCreateTable db dbId tableId

        openTable tableId = do
                db <- getDB
                sqOpenTable db tableId

        startTransaction = do
                db <- getDB
                sqStartTransaction db

        commitTransaction txn = do
                db <- getDB
                sqCommitTransaction db txn

        insertTuple txn table tuple = do
                db <- getDB
                sqInsertTuple table db txn tuple

        beginTableScan table = do
                db <- getDB
                sqBeginTableScan table db

        tableScanNext iterator dir = do
                db    <- getDB
                tuple <- sqTableScanNext iterator db dir
                return (iterator, tuple)

        getTupleData = sqTupleGetData

runSuziQ :: String -> SuziQ () -> IO ()
runSuziQ rootPath prog = do
        db <- sqCreateDB rootPath
        case db of
                (Just db) -> do
                        let initState = initSqState db
                        res <- runExceptT $ runStateT (unSuziQ prog) initState
                        case res of
                                Left err ->
                                        putStrLn
                                                $  "Uncaught storage error: "
                                                ++ show err
                                Right _ -> return ()
                _ -> putStrLn "cannot create database instance"


lastErrorMessage :: (MonadIO m) => m String
lastErrorMessage = do
        len <- liftIO sq_last_error_length
        msg <- liftIO $ allocaBytes (fromIntegral len) $ \ptr -> do
                len <- sq_last_error_message ptr (fromIntegral len)
                C.packCStringLen (ptr, fromIntegral len)
        return $ C.unpack msg

lastError :: (MonadIO m) => m SqErr
lastError = do
        msg <- lastErrorMessage
        return $ Msg msg

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
                else return Nothing


sqCreateTable :: SqDB -> OID -> OID -> SuziQ SqTable
sqCreateTable db dbId tableId = do
        table <- liftIO $ withForeignPtr db $ \database -> do
                ptr <- sq_create_table database
                                       (fromIntegral dbId)
                                       (fromIntegral tableId)
                if ptr /= nullPtr
                        then do
                                foreignPtr <- newForeignPtr sq_free_table ptr
                                return $ Just foreignPtr
                        else return Nothing

        case table of
                (Just table) -> return table
                _            -> do
                        err <- lastError
                        lerror err

sqOpenTable :: SqDB -> OID -> SuziQ (Maybe SqTable)
sqOpenTable db tableId = liftIO $ withForeignPtr db $ \database -> do
        ptr <- sq_open_table database (fromIntegral tableId)
        if ptr /= nullPtr
                then do
                        foreignPtr <- newForeignPtr sq_free_table ptr
                        return $ Just foreignPtr
                else return Nothing

sqStartTransaction :: SqDB -> SuziQ SqTransaction
sqStartTransaction db = do
        txn <- liftIO $ withForeignPtr db $ \database -> do
                ptr <- sq_start_transaction database
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

sqInsertTuple :: SqTable -> SqDB -> SqTransaction -> B.ByteString -> SuziQ ()
sqInsertTuple table db txn tuple = do
        result <- liftIO $ withForeignPtr db $ \database ->
                withForeignPtr table $ \table -> withForeignPtr txn $ \txn ->
                        B.useAsCStringLen tuple
                                $ \(buf, len) -> sq_table_insert_tuple
                                          table
                                          database
                                          txn
                                          buf
                                          (fromIntegral len)
        if fromIntegral result == 1
                then return ()
                else do
                        err <- lastError
                        lerror err

sqBeginTableScan :: SqTable -> SqDB -> SuziQ SqTableScanIterator
sqBeginTableScan table db = do
        iterator <- liftIO $ withForeignPtr db $ \database ->
                withForeignPtr table $ \table -> do
                        ptr <- sq_table_begin_scan table database

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
sqTableScanNext iterator db dir = do
        tuple <- liftIO $ withForeignPtr db $ \database ->
                withForeignPtr iterator $ \iterator -> do
                        ptr <- sq_table_scan_next
                                iterator
                                database
                                (fromIntegral $ getScanDirection dir)

                        if ptr /= nullPtr
                                then do
                                        foreignPtr <- newForeignPtr
                                                sq_free_tuple
                                                ptr
                                        return $ Just foreignPtr
                                else return Nothing

        case tuple of
                (Just tuple) -> return $ Just tuple
                _            -> do
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
