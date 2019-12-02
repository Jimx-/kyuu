{-# LANGUAGE FlexibleContexts, TemplateHaskell, RankNTypes #-}
module Kyuu.Config
        ( Config
        , defaultIsolationLevel_
        , getConfig
        )
where

import           Kyuu.Prelude
import           Kyuu.Storage.Backend

import           Control.Lens
import           Control.Monad.Reader

import           Data.Default.Class
import           Data.Has

data Config = Config { _defaultIsolationLevel :: IsolationLevel }

makeLensesWith (lensRules & lensField .~ lensGen) ''Config

getConfig :: (MonadIO m, MonadReader r m, Has Config r) => Iso' Config a -> m a
getConfig l = view l <$> asks getter

instance Default Config where
        def = Config ReadCommitted
