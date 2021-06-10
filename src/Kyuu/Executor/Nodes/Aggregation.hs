{-# LANGUAGE GADTs #-}

module Kyuu.Executor.Nodes.Aggregation
  ( buildAggregationOp,
  )
where

import Data.Hashable
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe (fromMaybe, listToMaybe)
import Kyuu.Core
import Kyuu.Executor.Accumulator
import Kyuu.Executor.Iterator
import Kyuu.Executor.Operators
import Kyuu.Expression
import Kyuu.Parse.Analyzer
import Kyuu.Prelude
import Kyuu.Value

type AggHashTable = Map Int [([Value], [Accumulator])]

updateAccumulators :: [Value] -> [[Value]] -> [Accumulator] -> AggHashTable -> AggHashTable
updateAccumulators keys vals accs ht =
  let hv = hash keys
      bucket = fromMaybe [] (Map.lookup hv ht)
      oldAccs = fromMaybe accs $ listToMaybe [accs | (v, accs) <- bucket, v == keys]
      newAccs = zipWith accumulateValues vals oldAccs
      newBucket = (keys, newAccs) : [(v, accs) | (v, accs) <- bucket, v /= keys]
   in Map.insert hv newBucket ht

buildAggregationOp :: (StorageBackend m) => Operator m -> Kyuu m (Operator m)
buildAggregationOp op@(AggregationOp aggs groupBys _ tupleDesc input) = do
  let accs = map getAccumulator aggs
  (ht, newInput) <- buildTable aggs groupBys accs Map.empty input
  let tuples = buildTuples tupleDesc $ concat $ Map.elems ht
  return op {tuples = tuples, input = newInput}
  where
    buildTable :: (StorageBackend m) => [AggregateDesc] -> [SqlExpr Value] -> [Accumulator] -> AggHashTable -> Operator m -> Kyuu m (AggHashTable, Operator m)
    buildTable aggs groupBys accs ht input = do
      (tuple, newInput) <- nextTuple input

      case tuple of
        Just tuple -> do
          keys <- forM groupBys $ \expr -> evalExpr expr tuple
          vals <- forM aggs $ \(AggregateDesc _ args) -> do
            forM args $ \expr -> evalExpr expr tuple

          buildTable
            aggs
            groupBys
            accs
            (updateAccumulators keys vals accs ht)
            newInput
        _ -> return (ht, newInput)

    buildTuples :: TupleDesc -> [([Value], [Accumulator])] -> [Tuple]
    buildTuples tupleDesc ((vals, accs) : xs) =
      let accTuple = Tuple (replicate (length accs) (ColumnDesc 0 0)) (map aggregateValue accs)
          tuple = accTuple <> Tuple tupleDesc vals
       in tuple : buildTuples tupleDesc xs
    buildTuples _ [] = []
