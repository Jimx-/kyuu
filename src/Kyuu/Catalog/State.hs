{-# LANGUAGE TemplateHaskell #-}
module Kyuu.Catalog.State
        ( CatalogState
        , tableSchemas_
        , initCatalogState
        )
where

import           Kyuu.Prelude
import           Kyuu.Catalog.Schema

import           Control.Lens

newtype CatalogState = CatalogState { _tableSchemas :: [TableSchema] }
                  deriving (Eq, Show)

makeLensesWith (lensRules & lensField .~ lensGen) ''CatalogState

initCatalogState :: CatalogState
initCatalogState = CatalogState []
