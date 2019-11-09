{-# LANGUAGE ForeignFunctionInterface #-}
module Kyuu.SuziQ.DB
        ( SqDB
        , SqTable
        , createDB
        , createTable
        , tableInsertTuple
        )
where

import           Kyuu.Prelude

import           Data.Int                       ( Int64 )
import qualified Data.ByteString               as B
import           Foreign.Ptr
import           Foreign.ForeignPtr
import           Foreign.C.Types                ( CChar )
import           Foreign.C.String               ( CString(..)
                                                , newCString
                                                , withCString
                                                )

data DBPtr
data TablePtr

type SqDB = ForeignPtr DBPtr
type SqTable = ForeignPtr TablePtr

foreign import ccall unsafe "sq_create_db"
  sq_create_db :: CString -> IO (Ptr DBPtr)

foreign import ccall unsafe "&sq_free_db"
  sq_free_db :: FunPtr (Ptr DBPtr -> IO ())

foreign import ccall unsafe "sq_create_table"
  sq_create_table :: Ptr DBPtr -> Int64 -> Int64 -> IO (Ptr TablePtr)

foreign import ccall unsafe "&sq_free_table"
  sq_free_table :: FunPtr (Ptr TablePtr -> IO ())

foreign import ccall unsafe "sq_table_insert_tuple"
  sq_table_insert_tuple :: Ptr TablePtr -> Ptr DBPtr -> Ptr CChar -> Int64 -> IO ()

createDB :: (MonadIO m) => String -> m (Maybe SqDB)
createDB rootPath = liftIO $ withCString rootPath $ \cstr -> do
        ptr <- sq_create_db cstr
        if ptr /= nullPtr
                then do
                        foreignPtr <- newForeignPtr sq_free_db ptr
                        return $ Just foreignPtr
                else return Nothing

createTable :: (MonadIO m) => SqDB -> OID -> OID -> m (Maybe SqTable)
createTable db dbId tableId = liftIO $ withForeignPtr db $ \database -> do
        ptr <- sq_create_table database
                               (fromIntegral dbId)
                               (fromIntegral tableId)
        if ptr /= nullPtr
                then do
                        foreignPtr <- newForeignPtr sq_free_table ptr
                        return $ Just foreignPtr
                else return Nothing

tableInsertTuple :: (MonadIO m) => SqTable -> SqDB -> B.ByteString -> m ()
tableInsertTuple table db tuple = liftIO $ withForeignPtr db $ \database ->
        withForeignPtr table $ \table ->
                B.useAsCStringLen tuple $ \(buf, len) -> do
                        sq_table_insert_tuple table
                                              database
                                              buf
                                              (fromIntegral len)
