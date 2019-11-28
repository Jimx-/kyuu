{-# LANGUAGE ConstraintKinds, FlexibleContexts, TemplateHaskell, MultiParamTypeClasses, TypeFamilies, GeneralizedNewtypeDeriving #-}
module Kyuu.Storage.SuziQ.Core
        ( SuziQ
        , unSuziQ
        , initSqState
        , getDB
        , lerror
        )
where

import           Control.Lens
import           Control.Monad.Base
import           Control.Monad.Trans.State.Lazy
import           Control.Monad.Trans.Except
import           Control.Monad.Trans.Class
import           Control.Monad.Trans.Control
import           Control.Monad.Signatures

import           Kyuu.Prelude            hiding ( get )
import           Kyuu.Storage.SuziQ.Error       ( SqErr(..) )
import           Kyuu.Storage.SuziQ.FFI

newtype SqState = SqState { _db :: SqDB }
            deriving (Eq, Show)

makeLensesWith (lensRules & lensField .~ lensGen) ''SqState

newtype SuziQ a = SuziQ { unSuziQ :: StateT SqState (ExceptT SqErr IO) a }
  deriving (Monad, Applicative, Functor, MonadBase IO, MonadIO)

instance MonadBaseControl IO SuziQ where
        type StM SuziQ a = (Either SqErr (a, SqState))
        liftBaseWith f = SuziQ $ liftBaseWith $ \q -> f (q . unSuziQ)
        restoreM = SuziQ . restoreM

liftCatchS :: Catch e (StateT SqState (ExceptT SqErr IO)) a -> Catch e SuziQ a
liftCatchS f m h = SuziQ $ f (unSuziQ m) (unSuziQ . h)

initSqState :: SqDB -> SqState
initSqState db = SqState { _db = db }

getDB :: SuziQ SqDB
getDB = SuziQ $ (^. db_) <$> get

lcatch :: SuziQ a -> (SqErr -> SuziQ a) -> SuziQ a
lcatch = liftCatchS (liftCatch catchE)

lerror :: SqErr -> SuziQ a
lerror = SuziQ . lift . throwE
