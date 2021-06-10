{-# LANGUAGE GADTs #-}

module Kyuu.Executor.Nodes.HashJoin
  ( buildHashJoinOp,
  )
where

import Kyuu.Core
import Kyuu.Executor.Iterator
import Kyuu.Executor.Operators
import Kyuu.Expression
import Kyuu.Prelude
import Kyuu.Value

buildHashJoinOp :: (StorageBackend m) => Operator m -> Kyuu m (Operator m)
buildHashJoinOp op@(HashJoinOp _ innerKeys _ _ _ _ innerInput) = do
  (hashTable, newInner) <- buildInner innerKeys emptyHashTable innerInput
  return op {hashTable = hashTable, innerInput = newInner}
  where
    buildInner :: (StorageBackend m) => [SqlExpr Value] -> HashTable -> Operator m -> Kyuu m (HashTable, Operator m)
    buildInner innerKeys hashTable innerOp = do
      (tuple, newInner) <- nextTuple innerOp

      case tuple of
        Just tuple -> do
          vals <- forM innerKeys $ \expr -> evalExpr expr tuple
          let newTable = insertHashTable vals tuple hashTable
          buildInner innerKeys newTable newInner
        _ -> return (hashTable, newInner)
