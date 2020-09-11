module Kyuu.Prelude
  ( lensGen,
    maybeM,
    OID,
    module X,
  )
where

import Control.Lens
import Control.Monad as X
  ( forM,
    forM_,
    guard,
    unless,
    void,
  )
import Control.Monad.IO.Class as X
  ( MonadIO (..),
  )
import Control.Monad.Reader as X
  ( MonadReader,
    runReaderT,
    withReaderT,
  )
import Control.Monad.State as X
  ( MonadState,
    get,
    modify,
    put,
    runStateT,
  )
import Language.Haskell.TH.Syntax

lensGen _ _ n = case nameBase n of
  '_' : x -> [TopName (mkName $ x ++ "_")]

maybeM :: Monad m => m b -> (a -> m b) -> m (Maybe a) -> m b
maybeM err f value = do
  x <- value
  case x of
    Just y -> f y
    Nothing -> err

type OID = Int
