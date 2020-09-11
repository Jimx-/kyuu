{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell #-}

module Kyuu.Core
  ( Kyuu,
    Transaction,
    initKyuuState,
    getKState,
    getCatalogState,
    modifyCatalogState,
    takeCatalogState,
    putCatalogState,
    lcatch,
    lerror,
    startTransaction,
    finishTransaction,
    getCurrentTransaction,
    requestCheckpoint,
    getNextOid,
    module X,
  )
where

import Control.Concurrent.STM
import Control.Concurrent.STM.TMVar
import Control.Lens
import Control.Monad.Except
import Control.Monad.Trans.Class
import Control.Monad.Trans.Control
import Control.Monad.Trans.Except
import Control.Monad.Trans.Reader (ReaderT)
import qualified Control.Monad.Trans.Reader as R
import Control.Monad.Trans.State.Lazy hiding
  ( get,
    liftCatch,
    modify,
  )
import qualified Control.Monad.Trans.State.Lazy as ST
import Data.Map (Map)
import qualified Data.Map as Map
import Kyuu.Catalog.State
import Kyuu.Config
import Kyuu.Error
import Kyuu.Prelude
import Kyuu.Storage.Backend as X
  ( StorageBackend,
  )
import qualified Kyuu.Storage.Backend as S

type Transaction m = S.TransactionType m

data KState m = KState
  { _catalogState :: TMVar CatalogState,
    _currentTxn :: Maybe (Transaction m),
    _checkpointRequestQueue :: TQueue ()
  }

makeLensesWith (lensRules & lensField .~ lensGen) ''KState

type Kyuu m = ReaderT Config (StateT (KState m) (ExceptT Err m))

initKyuuState :: TMVar CatalogState -> TQueue () -> KState m
initKyuuState mcs = KState mcs Nothing

getKState :: (StorageBackend m) => Kyuu m (KState m)
getKState = get

getCatalogState :: (StorageBackend m) => Kyuu m CatalogState
getCatalogState = do
  m <- (^. catalogState_) <$> get
  liftIO $ atomically $ readTMVar m

modifyCatalogState ::
  (StorageBackend m) => (CatalogState -> CatalogState) -> Kyuu m ()
modifyCatalogState f = do
  m <- (^. catalogState_) <$> get
  state <- liftIO $ atomically $ takeTMVar m
  liftIO $ atomically $ putTMVar m (f state)

takeCatalogState :: (StorageBackend m) => Kyuu m CatalogState
takeCatalogState = do
  m <- (^. catalogState_) <$> get
  liftIO $ atomically $ takeTMVar m

putCatalogState :: (StorageBackend m) => CatalogState -> Kyuu m ()
putCatalogState state = do
  m <- (^. catalogState_) <$> get
  liftIO $ atomically $ putTMVar m state

lcatch :: (StorageBackend m) => Kyuu m a -> (Err -> Kyuu m a) -> Kyuu m a
lcatch = R.liftCatch $ ST.liftCatch catchE

lerror :: (StorageBackend m) => Err -> Kyuu m a
lerror = lift . lift . throwE

startTransaction :: (StorageBackend m) => Kyuu m (Transaction m)
startTransaction = do
  currentTxn <- (^. currentTxn_) <$> get
  case currentTxn of
    (Just txn) -> return txn
    Nothing -> do
      isoLevel <- getConfig defaultIsolationLevel_
      txn <- S.startTransaction isoLevel
      modify $ set currentTxn_ (Just txn)
      return txn

finishTransaction :: (StorageBackend m) => Kyuu m ()
finishTransaction = do
  currentTxn <- (^. currentTxn_) <$> get
  case currentTxn of
    Nothing -> return ()
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
        ( InvalidState
            "get current transaction in invalid context"
        )

requestCheckpoint :: (StorageBackend m) => Kyuu m ()
requestCheckpoint =
  (^. checkpointRequestQueue_)
    <$> get
    >>= liftIO
      . atomically
      . flip writeTQueue ()

getNextOid :: (StorageBackend m) => Kyuu m OID
getNextOid = S.getNextOid
