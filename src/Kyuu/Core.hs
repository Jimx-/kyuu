{-# LANGUAGE ConstraintKinds, FlexibleContexts, TemplateHaskell #-}
module Kyuu.Core
        ( Kyuu
        , HasKState
        , getKState
        , getCatalogState
        , modifyCatalogState
        , lcatch
        , lerror
        , runKyuu
        , module X
        )
where

import           Kyuu.Error
import           Kyuu.Catalog.State
import           Kyuu.Prelude
import           Kyuu.Storage.Backend          as X
                                                ( StorageBackend )

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


data KState = KState { _catalogState :: CatalogState }
            deriving (Eq, Show)

makeLensesWith (lensRules & lensField .~ lensGen) ''KState

type Kyuu m = StateT KState (ExceptT Err m)

type HasKState m = (MonadState KState m)

initKyuuState :: KState
initKyuuState = KState initCatalogState

getKState :: (HasKState m) => m KState
getKState = get

getCatalogState :: (HasKState m) => m CatalogState
getCatalogState = (^. catalogState_) <$> get

modifyCatalogState :: (HasKState m) => (CatalogState -> CatalogState) -> m ()
modifyCatalogState f = modify $ over catalogState_ f

lcatch :: (StorageBackend m) => Kyuu m a -> (Err -> Kyuu m a) -> Kyuu m a
lcatch = liftCatch catchE

lerror :: (StorageBackend m) => Err -> Kyuu m a
lerror = lift . throwE

-- |Perform all effects produced by the query processor
runKyuu :: (StorageBackend m, MonadIO m) => Kyuu m () -> m ()
runKyuu prog = do
        res <- runExceptT $ runStateT prog initKyuuState
        case res of
                Left  err -> liftIO $ putStrLn $ "Uncaught error: " ++ show err
                Right _   -> return ()
