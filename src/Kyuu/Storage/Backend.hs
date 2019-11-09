{-# LANGUAGE TypeFamilies #-}
module Kyuu.Storage.Backend
        ( StorageBackend(..)
        )
where

import           Kyuu.Prelude

import           Control.Monad.Trans.State.Lazy
import           Control.Monad.Trans.Except
import           Control.Monad.Trans.Class

import qualified Data.ByteString               as B

class MonadIO m => StorageBackend m where
    type TableType m :: *
    createTable :: OID -> OID -> m (TableType m)
    insertTuple :: TableType m -> B.ByteString -> m ()

instance (StorageBackend m) => StorageBackend (StateT s m) where
        type TableType (StateT s m) = TableType m
        createTable dbId tableId = lift $ createTable dbId tableId
        insertTuple table tuple = lift $ insertTuple table tuple

instance (StorageBackend m) => StorageBackend (ExceptT e m) where
        type TableType (ExceptT e m) = TableType m
        createTable dbId tableId = lift $ createTable dbId tableId
        insertTuple table tuple = lift $ insertTuple table tuple
