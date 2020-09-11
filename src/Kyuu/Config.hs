{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Kyuu.Config
  ( Config,
    defaultIsolationLevel_,
    getConfig,
  )
where

import Control.Lens
import Control.Monad.Reader
import Data.Default.Class
import Data.Has
import Kyuu.Prelude
import Kyuu.Storage.Backend

data Config = Config {_defaultIsolationLevel :: IsolationLevel}

makeLensesWith (lensRules & lensField .~ lensGen) ''Config

getConfig :: (MonadIO m, MonadReader r m, Has Config r) => Lens' Config a -> m a
getConfig l = view l <$> asks getter

instance Default Config where
  def = Config ReadCommitted
