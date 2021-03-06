{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

module Kyuu.Planner.LogicalPlan
  ( JoinType (..),
    LogicalPlan (..),
    PlanBuilderState (..),
    isOptimizableQuery,
    getLogicalTupleDesc,
    buildLogicalPlan,
    resolveRangeTableRef,
  )
where

import Control.Lens
import Control.Monad.State.Lazy
import Kyuu.Catalog.Catalog
import Kyuu.Catalog.Schema
import Kyuu.Core
import Kyuu.Error
import Kyuu.Expression
import Kyuu.Index
import Kyuu.Parse.Analyzer
import Kyuu.Prelude
import Kyuu.Table
import Kyuu.Value

data JoinType = InnerJoin | LeftOuterJoin | RightOuterJoin | FullOuterJoin deriving (Eq, Show)

data LogicalPlan
  = DataSource
      { tableId :: OID,
        tableName :: String,
        searchArgs :: [SqlExpr Value],
        indexInfos :: [IndexSchema],
        tupleDesc :: TupleDesc
      }
  | Selection
      { conditions :: [SqlExpr Value],
        tupleDesc :: TupleDesc,
        childPlan :: LogicalPlan
      }
  | Projection
      { exprs :: [SqlExpr Value],
        tupleDesc :: TupleDesc,
        childPlan :: LogicalPlan
      }
  | Join
      { joinType :: JoinType,
        joinQuals :: [SqlExpr Value],
        otherQuals :: [SqlExpr Value],
        tupleDesc :: TupleDesc,
        leftChild :: LogicalPlan,
        rightChild :: LogicalPlan
      }
  | Aggregation
      { aggregates :: [AggregateDesc],
        groupBys :: [SqlExpr Value],
        tupleDesc :: TupleDesc,
        childPlan :: LogicalPlan
      }
  | Limit
      { limit :: Int,
        tupleDesc :: TupleDesc,
        childPlan :: LogicalPlan
      }
  | Offset
      { offset :: Int,
        tupleDesc :: TupleDesc,
        childPlan :: LogicalPlan
      }
  deriving (Show)

newtype PlanBuilderState = PlanBuilderState
  { _needPredicatePushDown :: Bool
  }
  deriving (Eq, Show)

makeLensesWith (lensRules & lensField .~ lensGen) ''PlanBuilderState

type PlanBuilder m a = StateT PlanBuilderState (Kyuu m) a

runPlanBuilder ::
  (StorageBackend m) => PlanBuilder m a -> Kyuu m (a, PlanBuilderState)
runPlanBuilder = flip runStateT $ PlanBuilderState False

setNeedPredicatePushDown :: (MonadState PlanBuilderState m) => m ()
setNeedPredicatePushDown = modify $ set needPredicatePushDown_ True

getLogicalTupleDesc :: LogicalPlan -> TupleDesc
getLogicalTupleDesc = tupleDesc

isOptimizableQuery :: Query -> Bool
isOptimizableQuery Query {_parseTree = SelectStmt {}} = True
isOptimizableQuery _ = False

buildLogicalPlan ::
  (StorageBackend m) => Query -> Kyuu m (LogicalPlan, PlanBuilderState)
buildLogicalPlan Query {_parseTree, _rangeTable, _aggregates} =
  runPlanBuilder $ buildStmt _parseTree _rangeTable _aggregates

buildStmt ::
  (StorageBackend m) =>
  ParserNode ->
  RangeTable ->
  [AggregateDesc] ->
  PlanBuilder m LogicalPlan
buildStmt SelectStmt {selectItems, fromItem, whereExpr, groupBys, havingExpr, offset, limit} rangeTable aggregates = do
  ds <- buildFromItem fromItem rangeTable
  sigma <- maybeM (return ds) (buildSelectFilter ds) $ return whereExpr
  let agg =
        if not (null groupBys) || not (null aggregates)
          then buildAggregation sigma aggregates groupBys
          else sigma
  sigma' <- maybeM (return agg) (buildSelectFilter agg) $ return havingExpr
  let pi = buildProjection sigma' selectItems
      off = maybe pi (buildOffset pi) offset
      lim = maybe off (buildLimit off) limit
  return lim

resolveRangeTableRef :: RangeTableRef -> RangeTable -> RangeTableEntry
resolveRangeTableRef (RangeTableRef r) table = table !! r

buildFromItem ::
  (StorageBackend m) =>
  RangeTableRef ->
  RangeTable ->
  PlanBuilder m LogicalPlan
buildFromItem ref table = buildFromTable table $ resolveRangeTableRef ref table

buildFromTable ::
  (StorageBackend m) =>
  RangeTable ->
  RangeTableEntry ->
  PlanBuilder m LogicalPlan
buildFromTable _ (RteTable tableId tableName) = do
  table <- lift $ openTable tableId

  case table of
    (Just table) -> do
      columns <- lift $ getTableColumns tableId
      tupleDesc <- forM columns $ \ColumnSchema {colTable, colId} ->
        return $ ColumnDesc colTable colId
      indexes <- lift $ openIndexes table
      size <- lift $ estimateTableSize table

      return $ DataSource tableId tableName [] (map indexSchema indexes) tupleDesc
    _ -> lift $ lerror $ TableNotFound tableId
buildFromTable table j@(RteJoin left right) = do
  lp <- buildFromItem left table
  rp <- buildFromItem right table
  return $ Join (getJoinType j) [] [] (tupleDesc lp ++ tupleDesc rp) lp rp

getJoinType :: RangeTableEntry -> JoinType
getJoinType (RteJoin _ _) = InnerJoin

buildSelectFilter ::
  (StorageBackend m) =>
  LogicalPlan ->
  SqlExpr Value ->
  PlanBuilder m LogicalPlan
buildSelectFilter child whereExpr = do
  setNeedPredicatePushDown
  let conditions = decomposeWhereFactors whereExpr
  return $ Selection conditions (tupleDesc child) child
  where
    decomposeWhereFactors :: SqlExpr Value -> [SqlExpr Value]
    decomposeWhereFactors (BinOpExpr BAnd lhs rhs) =
      decomposeWhereFactors lhs ++ decomposeWhereFactors rhs
    decomposeWhereFactors expr = [expr]

buildAggregation :: LogicalPlan -> [AggregateDesc] -> [SqlExpr Value] -> LogicalPlan
buildAggregation child aggs groupBys = Aggregation aggs groupBys tupleDesc child
  where
    tupleDesc =
      flip fmap groupBys $
        \expr ->
          ColumnDesc
            (getOutputTableId expr)
            (getOutputColumnId expr)

buildProjection :: LogicalPlan -> [SqlExpr Value] -> LogicalPlan
buildProjection child exprs = Projection exprs tupleDesc child
  where
    tupleDesc =
      flip fmap exprs $
        \expr ->
          ColumnDesc
            (getOutputTableId expr)
            (getOutputColumnId expr)

buildLimit :: LogicalPlan -> Int -> LogicalPlan
buildLimit child limit = Limit limit (getLogicalTupleDesc child) child

buildOffset :: LogicalPlan -> Int -> LogicalPlan
buildOffset child offset = Offset offset (getLogicalTupleDesc child) child
