{-# LANGUAGE NamedFieldPuns #-}
module Kyuu.Index
        ( Index(..)
        , IndexScanIterator(..)
        , insertIndex
        , module X
        )
where

import           Kyuu.Prelude
import           Kyuu.Core
import           Kyuu.Value
import           Kyuu.Error
import           Kyuu.Table
import           Kyuu.Expression
import           Kyuu.Catalog.Schema
import qualified Kyuu.Storage.Backend          as S
import           Kyuu.Storage.Backend          as X
                                                ( ScanDirection(..) )

import qualified Data.ByteString               as B

data Index m = Index { indexId :: OID
                     , indexSchema :: IndexSchema
                     , indexStorage :: S.IndexType m}

data IndexScanIterator m = IndexScanIterator { index :: Index m
                                             , iterator :: S.IndexScanIteratorType m }

keyComparator :: B.ByteString -> B.ByteString -> Maybe Ordering
keyComparator a b = case decodeTupleValues a of
        Nothing       -> Nothing
        (Just aTuple) -> case decodeTupleValues b of
                Nothing       -> Nothing
                (Just bTuple) -> Just $ compareTuple aTuple bTuple

insertIndex
        :: (StorageBackend m) => Index m -> Tuple -> TupleSlot m -> Kyuu m ()
insertIndex Index { indexStorage } key tupleSlot = do
        let keyBuf = encodeTuple key
        S.insertIndex indexStorage keyBuf keyComparator tupleSlot
