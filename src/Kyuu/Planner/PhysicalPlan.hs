{-# LANGUAGE NamedFieldPuns #-}

module Kyuu.Planner.PhysicalPlan
  ( PhysicalPlan (..),
    buildPhysicalPlanForQuery,
  )
where

import Kyuu.Catalog.Schema
import Kyuu.Core
import Kyuu.Expression
import Kyuu.Parse.Analyzer
import qualified Kyuu.Planner.LogicalPlan as L
import Kyuu.Prelude
import Kyuu.Value

data PhysicalPlan
  = TableScan
      { tableId :: OID,
        tableName :: String,
        filters :: [SqlExpr Value],
        tupleDesc :: TupleDesc
      }
  | IndexScan
      { tableId :: OID,
        indexSchema :: IndexSchema,
        filters :: [SqlExpr Value],
        indexQuals :: [SqlExpr Value],
        tupleDesc :: TupleDesc
      }
  | Selection
      { conds :: [SqlExpr Value],
        tupleDesc :: TupleDesc,
        child :: PhysicalPlan
      }
  | Projection
      { exprs :: [SqlExpr Value],
        tupleDesc :: TupleDesc,
        child :: PhysicalPlan
      }
  | Aggregation
      { aggregates :: [AggregateDesc],
        groupBys :: [SqlExpr Value],
        tupleDesc :: TupleDesc,
        child :: PhysicalPlan
      }
  | NestedLoopJoin
      { joinType :: L.JoinType,
        joinQuals :: [SqlExpr Value],
        otherQuals :: [SqlExpr Value],
        tupleDesc :: TupleDesc,
        leftChild :: PhysicalPlan,
        rightChild :: PhysicalPlan
      }
  | HashJoin
      { joinType :: L.JoinType,
        leftKeys :: [SqlExpr Value],
        rightKeys :: [SqlExpr Value],
        tupleDesc :: TupleDesc,
        leftChild :: PhysicalPlan,
        rightChild :: PhysicalPlan
      }
  | Limit
      { limit :: Int,
        tupleDesc :: TupleDesc,
        child :: PhysicalPlan
      }
  | Offset
      { offset :: Int,
        tupleDesc :: TupleDesc,
        child :: PhysicalPlan
      }
  | CreateTable {tableSchema :: TableSchema}
  | Insert
      { tableId :: OID,
        targetExprs :: [[SqlExpr Tuple]]
      }
  deriving (Show)

buildPhysicalPlanForQuery :: (StorageBackend m) => Query -> Kyuu m PhysicalPlan
buildPhysicalPlanForQuery Query {_parseTree, _rangeTable} =
  buildSimpleStmt _parseTree _rangeTable

buildSimpleStmt ::
  (StorageBackend m) => ParserNode -> RangeTable -> Kyuu m PhysicalPlan
buildSimpleStmt (CreateTableStmt tableName columns constraints) _ = do
  let schema = TableSchema 0 tableName columns
  return $ CreateTable schema
buildSimpleStmt (InsertStmt ref targets exprList) rangeTable = do
  let (RteTable tableId _) = L.resolveRangeTableRef ref rangeTable
  targetExprs <- forM exprList $ \exprs -> buildAssignExprs targets exprs
  return $ Insert tableId targetExprs
  where
    buildAssignExprs targets exprs =
      forM (zip targets exprs) $ \(target, expr) ->
        return $
          ColumnAssignExpr
            (getOutputTableId target)
            (getOutputColumnId target)
            expr
