{-# LANGUAGE StandaloneDeriving, FlexibleContexts, UndecidableInstances #-}
module Kyuu.Executor.Builder
        ( ExecutionPlan(..)
        , buildExecPlan
        )
where

import           Kyuu.Core
import           Kyuu.Executor.Operators
import           Kyuu.Planner.PhysicalPlan

import           Kyuu.Storage.Backend

newtype ExecutionPlan m = ExecutionPlan { _root :: Operator m } deriving (Show)

buildExecPlan :: (StorageBackend m) => PhysicalPlan -> Kyuu m (ExecutionPlan m)
buildExecPlan pp = do
        op <- buildOperator pp
        let root = PrintOp True (getOpTupleDesc op) op
        return $ ExecutionPlan root

buildOperator :: (StorageBackend m) => PhysicalPlan -> Kyuu m (Operator m)

buildOperator (TableScan tableId tableName filters tupleDesc) =
        return $ TableScanOp tableId filters tupleDesc Nothing

buildOperator (Selection conds schema child) = do
        childOp <- buildOperator child
        return $ SelectionOp conds schema childOp

buildOperator (Projection exprs schema child) = do
        childOp <- buildOperator child
        return $ ProjectionOp exprs schema childOp

buildOperator (NestedLoopJoin _ joinQuals _ schema left right) = do
        leftOp  <- buildOperator left
        rightOp <- buildOperator right
        return $ NestLoopOp joinQuals schema leftOp rightOp

buildOperator (CreateTable schema) = return $ CreateTableOp schema False

buildOperator (Insert tableId targetExprs) =
        return $ InsertOp tableId targetExprs
