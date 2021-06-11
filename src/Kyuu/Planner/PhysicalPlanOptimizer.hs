{-# LANGUAGE GADTs #-}

module Kyuu.Planner.PhysicalPlanOptimizer
  ( getPhysicalPlan,
  )
where

import Kyuu.Catalog.Schema
import Kyuu.Core
import Kyuu.Expression
import Kyuu.Parse.Analyzer
import Kyuu.Planner.DataSource
import qualified Kyuu.Planner.LogicalPlan as L
import Kyuu.Planner.Path
import qualified Kyuu.Planner.PhysicalPlan as P
import Kyuu.Prelude
import Kyuu.Value

-- | Pick the best physical plan among all possible ones for a logical plan
getPhysicalPlan :: (StorageBackend m) => L.LogicalPlan -> Kyuu m P.PhysicalPlan
getPhysicalPlan lp = do
  ps <- genPhysicalPlans lp
  return $ head ps

-- | Generate all possible physical plans for a logical plan
genPhysicalPlans :: (StorageBackend m) => L.LogicalPlan -> Kyuu m [P.PhysicalPlan]
genPhysicalPlans ds@(L.DataSource tableId tableName sArgs indexInfos schema) = do
  let indexPaths = buildIndexPaths ds
  let scanPath = mkScanPath tableId tableName sArgs
  let allPaths = indexPaths ++ [scanPath]
  return $ map (`createScanPlan` schema) allPaths
genPhysicalPlans (L.Selection conds schema child) = do
  childPlan <- getPhysicalPlan child
  return [P.Selection conds schema childPlan]
genPhysicalPlans (L.Projection exprs schema child) = do
  childPlan <- getPhysicalPlan child
  return [P.Projection exprs schema childPlan]
genPhysicalPlans (L.Aggregation aggs groupBys schema child) = do
  childPlan <- getPhysicalPlan child
  return [P.Aggregation aggs groupBys schema childPlan]
genPhysicalPlans lp@L.Join {L.otherQuals = []} = do
  hashJoin <- genHashJoin lp
  return [hashJoin]
genPhysicalPlans lp@L.Join {L.otherQuals = otherQuals, L.tupleDesc = schema} = do
  nestedLoopJoin <- genNestedLoopJoin lp
  let selection = P.Selection otherQuals schema nestedLoopJoin
  return [selection]
genPhysicalPlans lp@(L.Limit limit schema child) = do
  childPlan <- getPhysicalPlan child
  return [P.Limit limit schema childPlan]
genPhysicalPlans lp@(L.Offset offset schema child) = do
  childPlan <- getPhysicalPlan child
  return [P.Offset offset schema childPlan]

genNestedLoopJoin :: (StorageBackend m) => L.LogicalPlan -> Kyuu m P.PhysicalPlan
genNestedLoopJoin (L.Join joinType joinQuals otherQuals schema left right) = do
  leftPlan <- getPhysicalPlan left
  rightPlan <- getPhysicalPlan right
  return $ P.NestedLoopJoin joinType joinQuals otherQuals schema leftPlan rightPlan

genHashJoin :: (StorageBackend m) => L.LogicalPlan -> Kyuu m P.PhysicalPlan
genHashJoin (L.Join joinType joinQuals _ schema left right) = do
  let leftKeys = map (\(BinOpExpr BEqual lhs _) -> lhs) joinQuals
      rightKeys = map (\(BinOpExpr BEqual _ rhs) -> rhs) joinQuals
  leftPlan <- getPhysicalPlan left
  rightPlan <- getPhysicalPlan right
  return $ P.HashJoin joinType leftKeys rightKeys schema leftPlan rightPlan
