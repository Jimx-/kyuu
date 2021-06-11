{-# LANGUAGE NamedFieldPuns #-}

module Kyuu.Executor.Executor
  ( executePlan,
  )
where

import Control.Lens
import Control.Monad.State.Lazy
import Data.List (find)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Kyuu.Catalog.Catalog
import Kyuu.Catalog.Schema
import Kyuu.Core
import Kyuu.Error
import Kyuu.Executor.Builder
import Kyuu.Executor.Iterator
import Kyuu.Executor.Nodes.Aggregation
import Kyuu.Executor.Nodes.HashJoin
import Kyuu.Executor.Nodes.IndexScan
import Kyuu.Executor.Operators
import Kyuu.Expression
import Kyuu.Index
import Kyuu.Prelude
import Kyuu.Table
import Kyuu.Value

executePlan :: (StorageBackend m) => ExecutionPlan m -> Kyuu m ()
executePlan (ExecutionPlan plan) = do
  op <- open plan
  op <- drain op
  close op

drain :: (StorageBackend m) => Operator m -> Kyuu m (Operator m)
drain op = do
  (tuple, newOp) <- nextTuple op

  case tuple of
    Just tuple -> drain newOp
    _ -> return newOp

open :: (StorageBackend m) => Operator m -> Kyuu m (Operator m)
open op@(TableScanOp tableId _ _ _) = do
  schema <- lookupTableById tableId
  table <- openTable tableId
  case (schema, table) of
    (Just schema, Just table) -> do
      iterator <- beginTableScan table
      return $ op {scanIterator = Just iterator}
    _ -> lerror (TableNotFound tableId)
open op@IndexScanOp {} = initIndexScanOp op
open op@SelectionOp {input = input} = do
  newInput <- open input
  return op {input = newInput}
open op@ProjectionOp {input = input} = do
  newInput <- open input
  return op {input = newInput}
open op@AggregationOp {input = input} = do
  newInput <- open input
  buildAggregationOp op {input = newInput}
open op@NestLoopOp {outerInput = outerInput, innerInput = innerInput} = do
  newOuter <- open outerInput
  newInner <- open innerInput
  return op {outerInput = newOuter, innerInput = newInner}
open op@HashJoinOp {outerInput = outerInput, innerInput = innerInput} = do
  newOuter <- open outerInput
  newInner <- open innerInput
  buildHashJoinOp op {outerInput = newOuter, innerInput = newInner}
open op@LimitOp {input = input} = do
  newInput <- open input
  return op {input = newInput}
open op@OffsetOp {input = input} = do
  newInput <- open input
  return op {input = newInput}
open op@PrintOp {input = input} = do
  newInput <- open input
  return op {input = newInput}
open op = return op

close :: (StorageBackend m) => Operator m -> Kyuu m ()
close op@IndexScanOp {} = closeIndexScanOp op
close _ = return ()
