{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NamedFieldPuns #-}

module Kyuu.Planner.DataSource
  ( buildIndexPaths,
    createScanPlan,
  )
where

import Data.Foldable (asum, toList)
import Kyuu.Catalog.Schema
import Kyuu.Core
import Kyuu.Expression
import qualified Kyuu.Planner.LogicalPlan as L
import Kyuu.Planner.Path
import qualified Kyuu.Planner.PhysicalPlan as P
import Kyuu.Prelude
import Kyuu.Value

-- | Build all possible index paths for a DataSource node.
buildIndexPaths :: L.LogicalPlan -> [Path]
buildIndexPaths (L.DataSource tableId _ sArgs indexInfos _) = flip map indexInfos $ \indexSchema ->
  mkIndexPath
    tableId
    indexSchema
    (concat $ matchClausesToIndex sArgs indexSchema)
    sArgs
  where
    matchClausesToIndex :: [SqlExpr Value] -> IndexSchema -> [[SqlExpr Value]]
    matchClausesToIndex clauses IndexSchema {indexTableId, colNums} = flip map colNums $ \colNum ->
      concatMap (toList . matchClauseToIndexCol indexTableId colNum) clauses

    matchClauseToIndexCol :: OID -> OID -> SqlExpr Value -> Maybe (SqlExpr Value)
    matchClauseToIndexCol tableId colNum (BinOpExpr op lhs rhs) = asum [(\lhs -> BinOpExpr op lhs rhs) <$> matchClauseToIndexCol tableId colNum lhs, (\commutator rhs -> BinOpExpr commutator rhs lhs) <$> getCommutator op <*> matchClauseToIndexCol tableId colNum rhs]
    matchClauseToIndexCol tableId colNum colRef@(ColumnRefExpr tableId' colNum')
      | tableId == tableId' && colNum == colNum' = Just colRef
    matchClauseToIndexCol _ _ _ = Nothing

createScanPlan :: Path -> TupleDesc -> P.PhysicalPlan
createScanPlan ScanPath {tableId, tableName, searchArgs} tupleDesc =
  P.TableScan tableId tableName searchArgs tupleDesc
createScanPlan IndexPath {tableId, indexSchema, indexClauses, searchArgs} tupleDesc =
  P.IndexScan tableId indexSchema searchArgs indexClauses tupleDesc
