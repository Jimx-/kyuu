{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE UndecidableInstances #-}

module Kyuu.Executor.Builder
  ( ExecutionPlan (..),
    buildExecPlan,
  )
where

import Data.MultiMap
import Kyuu.Catalog.Schema
import Kyuu.Core
import Kyuu.Executor.Operators
import Kyuu.Planner.PhysicalPlan
import Kyuu.Storage.Backend

newtype ExecutionPlan m = ExecutionPlan {_root :: Operator m} deriving (Show)

buildExecPlan :: (StorageBackend m) => PhysicalPlan -> Kyuu m (ExecutionPlan m)
buildExecPlan pp = do
  op <- buildOperator pp
  let root = PrintOp True (getOpTupleDesc op) op
  return $ ExecutionPlan root

buildOperator :: (StorageBackend m) => PhysicalPlan -> Kyuu m (Operator m)
buildOperator (TableScan tableId _ filters tupleDesc) =
  return $ TableScanOp tableId filters tupleDesc Nothing
buildOperator (IndexScan tableId IndexSchema {indexId} filters indexQuals tupleDesc) =
  return $ IndexScanOp tableId indexId filters indexQuals tupleDesc Nothing
buildOperator (Selection conds schema child) = do
  childOp <- buildOperator child
  return $ SelectionOp conds schema childOp
buildOperator (Projection exprs schema child) = do
  childOp <- buildOperator child
  return $ ProjectionOp exprs schema childOp
buildOperator (Aggregation aggs groupBys schema child) = do
  childOp <- buildOperator child
  return $ mkAggregationOp aggs groupBys schema childOp
buildOperator (NestedLoopJoin _ joinQuals _ schema left right) = do
  leftOp <- buildOperator left
  rightOp <- buildOperator right
  return $ mkNestLoopJoinOp joinQuals schema leftOp rightOp
buildOperator (HashJoin _ leftKeys rightKeys schema left right) = do
  leftOp <- buildOperator left
  rightOp <- buildOperator right
  return $ mkHashJoinOp leftKeys rightKeys schema leftOp rightOp
buildOperator (Offset offset schema child) = do
  childOp <- buildOperator child
  return $ OffsetOp offset schema childOp
buildOperator (Limit limit schema child) = do
  childOp <- buildOperator child
  return $ LimitOp limit schema childOp
buildOperator (CreateTable schema) = return $ CreateTableOp schema False
buildOperator (Insert tableId targetExprs) =
  return $ InsertOp tableId targetExprs
