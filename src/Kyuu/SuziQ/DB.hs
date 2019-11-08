{-# LANGUAGE ForeignFunctionInterface #-}
module Kyuu.SuziQ.DB
        ( createDB
        , createTable
        )
where

import           Kyuu.Prelude

import           Data.Int                       ( Int64 )
import           Foreign.Ptr
import           Foreign.ForeignPtr
import           Foreign.C.String               ( CString(..)
                                                , newCString
                                                , withCString
                                                )

data DBPtr
data TablePtr

foreign import ccall unsafe "sq_create_db"
  sq_create_db :: CString -> IO (Ptr DBPtr)

foreign import ccall unsafe "&sq_free_db"
  sq_free_db :: FunPtr (Ptr DBPtr -> IO ())

foreign import ccall unsafe "sq_create_table"
  sq_create_table :: Ptr DBPtr -> Int64 -> Int64 -> IO (Ptr TablePtr)

foreign import ccall unsafe "&sq_free_table"
  sq_free_table :: FunPtr (Ptr TablePtr -> IO ())

createDB :: (MonadIO m) => String -> m (Maybe (ForeignPtr DBPtr))
createDB rootPath = liftIO $ withCString rootPath $ \cstr -> do
        ptr <- sq_create_db cstr
        if ptr /= nullPtr
                then do
                        foreignPtr <- newForeignPtr sq_free_db ptr
                        return $ Just foreignPtr
                else return Nothing

createTable
        :: (MonadIO m)
        => (ForeignPtr DBPtr)
        -> OID
        -> OID
        -> m (Maybe (ForeignPtr TablePtr))
createTable db dbId tableId = liftIO $ withForeignPtr db $ \database -> do
        ptr <- sq_create_table database
                               (fromIntegral dbId)
                               (fromIntegral tableId)
        if ptr /= nullPtr
                then do
                        foreignPtr <- newForeignPtr sq_free_table ptr
                        return $ Just foreignPtr
                else return Nothing
