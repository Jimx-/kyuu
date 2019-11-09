{-# LANGUAGE ConstraintKinds, FlexibleContexts, TemplateHaskell #-}
module Kyuu.Core
        ( Kyuu
        , lcatch
        , lerror
        , runKyuu
        )
where

import           Kyuu.Error
import           Kyuu.Catalog.Schema
import           Kyuu.Prelude            hiding ( get )
import           Kyuu.Storage.Backend

import           Control.Lens
import           Control.Monad.Trans.State.Lazy
import           Control.Monad.Trans.Except
import           Control.Monad.Except
import           Control.Monad.Trans.Class

import           Data.Map                       ( Map )
import qualified Data.Map                      as Map

data CatalogState = CatalogState { _tableSchemas :: [TableSchema] }
                  deriving (Eq, Show)

data KState = KState { _catalogState :: CatalogState }
            deriving (Eq, Show)

makeLensesWith (lensRules & lensField .~ lensGen) ''CatalogState
makeLensesWith (lensRules & lensField .~ lensGen) ''KState

type Kyuu m = StateT KState (ExceptT Err m)

initKyuuState :: KState
initKyuuState = KState (CatalogState [])

getKState :: (StorageBackend m) => Kyuu m KState
getKState = get

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
