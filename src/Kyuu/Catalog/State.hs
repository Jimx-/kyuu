{-# LANGUAGE TemplateHaskell #-}

module Kyuu.Catalog.State
  ( CatalogState,
    tableSchemas_,
    indexSchemas_,
    initCatalogState,
  )
where

import Control.Lens
import Kyuu.Catalog.Schema
import Kyuu.Prelude
import Kyuu.Storage.Backend

data CatalogState = CatalogState
  { _tableSchemas :: [TableSchema],
    _indexSchemas :: [IndexSchema]
  }
  deriving (Eq, Show)

makeLensesWith (lensRules & lensField .~ lensGen) ''CatalogState

initCatalogState :: CatalogState
initCatalogState = CatalogState [] []
