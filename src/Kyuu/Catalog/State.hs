{-# LANGUAGE TemplateHaskell #-}
module Kyuu.Catalog.State
        ( CatalogState
        , tableSchemas_
        , indexSchemas_
        , initCatalogState
        )
where

import           Kyuu.Prelude
import           Kyuu.Catalog.Schema
import           Kyuu.Storage.Backend

import           Control.Lens

data CatalogState = CatalogState { _tableSchemas :: [TableSchema]
                                 , _indexSchemas :: [IndexSchema] }
                  deriving (Eq, Show)

makeLensesWith (lensRules & lensField .~ lensGen) ''CatalogState

initCatalogState :: CatalogState
initCatalogState = CatalogState [] []
