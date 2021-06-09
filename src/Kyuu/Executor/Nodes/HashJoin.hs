{-# LANGUAGE GADTs #-}

module Kyuu.Executor.Nodes.HashJoin
  ( buildHashJoinOp,
  )
where

import Control.Lens
import Control.Monad.State.Lazy
import Data.Hashable
import Data.List (find)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import qualified Data.MultiMap as MM
import Kyuu.Catalog.Catalog
import Kyuu.Catalog.Schema
import Kyuu.Core
import Kyuu.Error
import Kyuu.Executor.Builder
import Kyuu.Executor.Iterator
import Kyuu.Executor.Operators
import Kyuu.Expression
import Kyuu.Index
import Kyuu.Prelude
import Kyuu.Table
import Kyuu.Value

buildHashJoinOp :: (StorageBackend m) => Operator m -> Kyuu m (Operator m)
buildHashJoinOp op@(HashJoinOp _ innerKeys _ _ _ _ innerInput) = do
  (hashTable, newInner) <- buildInner innerKeys MM.empty innerInput
  return op {hashTable = hashTable, innerInput = newInner}
  where
    buildInner :: (StorageBackend m) => [SqlExpr Value] -> HashTable -> Operator m -> Kyuu m (HashTable, Operator m)
    buildInner innerKeys hashTable innerOp = do
      (tuple, newInner) <- nextTuple innerOp

      case tuple of
        Just tuple -> do
          vals <- forM innerKeys $ \expr -> evalExpr expr tuple
          let newTable = MM.insert (hash vals) (vals, tuple) hashTable
          buildInner innerKeys newTable newInner
        _ -> return (hashTable, newInner)
