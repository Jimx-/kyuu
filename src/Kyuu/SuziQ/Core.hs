{-# LANGUAGE ConstraintKinds, FlexibleContexts, TemplateHaskell #-}
module Kyuu.SuziQ.Core
        ( SuziQ
        , initSqState
        , getDB
        , lerror
        )
where

import           Control.Lens
import           Control.Monad.Trans.State.Lazy
import           Control.Monad.Trans.Except
import qualified Control.Monad.Trans.Identity  as I
import           Control.Monad.Trans.Class

import           Kyuu.Prelude            hiding ( get )
import           Kyuu.SuziQ.Error               ( SqErr(..) )
import           Kyuu.SuziQ.FFI

newtype SqState = SqState { _db :: SqDB }
            deriving (Eq, Show)

makeLensesWith (lensRules & lensField .~ lensGen) ''SqState

type SuziQ = I.IdentityT (StateT SqState (ExceptT SqErr IO))

initSqState :: SqDB -> SqState
initSqState db = SqState { _db = db }

getDB :: SuziQ SqDB
getDB = lift $ (^. db_) <$> get

lcatch :: SuziQ a -> (SqErr -> SuziQ a) -> SuziQ a
lcatch = I.liftCatch (liftCatch catchE)

lerror :: SqErr -> SuziQ a
lerror = lift . lift . throwE
