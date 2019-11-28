{-# LANGUAGE ConstraintKinds, FlexibleContexts, TemplateHaskell #-}
module Kyuu.Core
        ( Kyuu
        , Transaction
        , initKyuuState
        , getKState
        , getCatalogState
        , modifyCatalogState
        , lcatch
        , lerror
        , startTransaction
        , finishTransaction
        , getCurrentTransaction
        , requestCheckpoint
        , module X
        )
where

import           Kyuu.Error
import           Kyuu.Catalog.State
import           Kyuu.Prelude
import qualified Kyuu.Storage.Backend          as S
import           Kyuu.Storage.Backend          as X
                                                ( StorageBackend )

import           Control.Concurrent.STM
import           Control.Concurrent.STM.TVar

import           Control.Lens
import           Control.Monad.Trans.State.Lazy
                                         hiding ( get
                                                , modify
                                                )
import           Control.Monad.Trans.Except
import           Control.Monad.Except
import           Control.Monad.Trans.Class

import           Data.Map                       ( Map )
import qualified Data.Map                      as Map

type Transaction m = S.TransactionType m

data KState m = KState { _catalogState :: TVar CatalogState
                       , _currentTxn :: Maybe (Transaction m)
                       , _checkpointRequestQueue :: TQueue () }

makeLensesWith (lensRules & lensField .~ lensGen) ''KState

type Kyuu m = StateT (KState m) (ExceptT Err m)

initKyuuState :: TVar CatalogState -> TQueue () -> KState m
initKyuuState mcs = KState mcs Nothing

getKState :: (StorageBackend m) => Kyuu m (KState m)
getKState = get

getCatalogState :: (StorageBackend m) => Kyuu m CatalogState
getCatalogState = do
        m <- (^. catalogState_) <$> get
        liftIO $ readTVarIO m

modifyCatalogState
        :: (StorageBackend m) => (CatalogState -> CatalogState) -> Kyuu m ()
modifyCatalogState f = do
        m <- (^. catalogState_) <$> get
        liftIO $ atomically $ modifyTVar m f

lcatch :: (StorageBackend m) => Kyuu m a -> (Err -> Kyuu m a) -> Kyuu m a
lcatch = liftCatch catchE

lerror :: (StorageBackend m) => Err -> Kyuu m a
lerror = lift . throwE

startTransaction :: (StorageBackend m) => Kyuu m (Transaction m)
startTransaction = do
        currentTxn <- (^. currentTxn_) <$> get
        case currentTxn of
                (Just txn) -> return txn
                Nothing    -> do
                        txn <- S.startTransaction
                        modify $ set currentTxn_ (Just txn)
                        return txn

finishTransaction :: (StorageBackend m) => Kyuu m ()
finishTransaction = do
        currentTxn <- (^. currentTxn_) <$> get
        case currentTxn of
                Nothing    -> return ()
                (Just txn) -> do
                        S.commitTransaction txn
                        modify $ set currentTxn_ Nothing
                        return ()

getCurrentTransaction :: (StorageBackend m) => Kyuu m (Transaction m)
getCurrentTransaction = do
        currentTxn <- (^. currentTxn_) <$> get
        case currentTxn of
                (Just txn) -> return txn
                Nothing ->
                        lerror
                                (InvalidState
                                        "get current transaction in invalid context"
                                )

requestCheckpoint :: (StorageBackend m) => Kyuu m ()
requestCheckpoint =
        (^. checkpointRequestQueue_)
                <$> get
                >>= liftIO
                .   atomically
                .   flip writeTQueue ()
