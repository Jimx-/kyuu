{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, TypeFamilies #-}
module Kyuu.SuziQ.Backend
        ( runSuziQ
        )
where

import           Kyuu.Prelude
import           Kyuu.Storage.Backend
import           Kyuu.SuziQ.FFI
import           Kyuu.SuziQ.Core
import           Kyuu.SuziQ.Error

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

instance StorageBackend SuziQ where
        type TableType SuziQ = SqTable
        createTable dbId tableId = do
                db <- getDB
                sqCreateTable db dbId tableId

        insertTuple table tuple = do
                db <- getDB
                sqInsertTuple table db tuple

runSuziQ :: String -> SuziQ () -> IO ()
runSuziQ rootPath prog = do
        db <- sqCreateDB rootPath
        case db of
                (Just db) -> do
                        let initState = initSqState db
                        res <- runExceptT
                                $ runStateT (runIdentityT prog) initState
                        case res of
                                Left err ->
                                        putStrLn
                                                $  "Uncaught error: "
                                                ++ show err
                                Right _ -> return ()
                _ -> putStrLn "cannot create database instance"


lastError :: (MonadIO m) => m SqErr
lastError = do
        len <- liftIO sq_last_error_length
        let msgbuf = C.pack (replicate (fromIntegral len) (chr 0))
        msg <- liftIO $ C.useAsCStringLen msgbuf $ \(buf, len) -> do
                len <- sq_last_error_message buf (fromIntegral len)
                C.packCString buf
        return (Msg (C.unpack msg))


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

sqInsertTuple :: SqTable -> SqDB -> B.ByteString -> SuziQ ()
sqInsertTuple table db tuple = do
        result <- liftIO $ withForeignPtr db $ \database ->
                withForeignPtr table $ \table ->
                        B.useAsCStringLen tuple
                                $ \(buf, len) -> sq_table_insert_tuple
                                          table
                                          database
                                          buf
                                          (fromIntegral len)
        if fromIntegral result == 1
                then return ()
                else do
                        err <- lastError
                        lerror err
