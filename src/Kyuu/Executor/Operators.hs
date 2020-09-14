{-# LANGUAGE DuplicateRecordFields #-}

module Kyuu.Executor.Operators
  ( Operator (..),
    getOpTupleDesc,
  )
where

import Kyuu.Catalog.Catalog
import Kyuu.Catalog.Schema
import Kyuu.Core
import Kyuu.Expression
import Kyuu.Index
import Kyuu.Prelude
import Kyuu.Table
import Kyuu.Value

data Operator m
  = TableScanOp
      { tableId :: OID,
        filters :: [SqlExpr Value],
        tupleDesc :: TupleDesc,
        scanIterator :: Maybe (TableScanIterator m)
      }
  | IndexScanOp
      { tableId :: OID,
        indexId :: OID,
        filters :: [SqlExpr Value],
        indexQuals :: [SqlExpr Value],
        tupleDesc :: TupleDesc,
        indexScanIterator :: Maybe (IndexScanIterator m)
      }
  | SelectionOp
      { filter :: [SqlExpr Value],
        tupleDesc :: TupleDesc,
        input :: Operator m
      }
  | ProjectionOp
      { columns :: [SqlExpr Value],
        tupleDesc :: TupleDesc,
        input :: Operator m
      }
  | NestLoopOp
      { joinQuals :: [SqlExpr Value],
        tupleDesc :: TupleDesc,
        outerInput :: Operator m,
        innerInput :: Operator m
      }
  | PrintOp
      { printHeader :: Bool,
        tupleDesc :: TupleDesc,
        input :: Operator m
      }
  | CreateTableOp
      { tableSchema :: TableSchema,
        done :: Bool
      }
  | InsertOp
      { tableId :: OID,
        targetExprs :: [[SqlExpr Tuple]]
      }

instance Show (Operator m) where
  show (TableScanOp tId filters tupleDesc _) =
    "TableScanOp {tableId = "
      ++ show tId
      ++ ", filters = "
      ++ show filters
      ++ ", tupleDesc = "
      ++ show tupleDesc
      ++ "}"
  show (IndexScanOp tId indexId filters indexQuals tupleDesc _) =
    "IndexScanOp {tableId = "
      ++ show tId
      ++ ", indexId = "
      ++ show indexId
      ++ ", filters = "
      ++ show filters
      ++ ", indexQuals = "
      ++ show indexQuals
      ++ ", tupleDesc = "
      ++ show tupleDesc
      ++ "}"
  show (SelectionOp filter tupleDesc input) =
    "SelectionOp {filter = "
      ++ show filter
      ++ ", tupleDesc = "
      ++ show tupleDesc
      ++ ", input = "
      ++ show input
      ++ "}"
  show (ProjectionOp columns tupleDesc input) =
    "ProjectionOp {columns = "
      ++ show columns
      ++ ", tupleDesc = "
      ++ show tupleDesc
      ++ ", input = "
      ++ show input
      ++ "}"
  show (NestLoopOp joinQuals tupleDesc outerInput innerInput) =
    "NestLoopOp {joinQuals = "
      ++ show joinQuals
      ++ ", tupleDesc = "
      ++ show tupleDesc
      ++ ", outerInput = "
      ++ show outerInput
      ++ ", innerInput = "
      ++ show innerInput
      ++ "}"
  show (PrintOp printHeader tupleDesc input) =
    "PrintOp {printHeader = "
      ++ show printHeader
      ++ ", tupleDesc = "
      ++ show tupleDesc
      ++ ", input = "
      ++ show input
      ++ "}"
  show (CreateTableOp tableSchema done) =
    "CreateTableOp {schema = "
      ++ show tableSchema
      ++ ", done = "
      ++ show done
      ++ "}"
  show (InsertOp tableId targetExprs) =
    "InsertOp {tableId = "
      ++ show tableId
      ++ ", targetExprs = "
      ++ show targetExprs
      ++ "}"

getOpTupleDesc :: Operator m -> TupleDesc
getOpTupleDesc CreateTableOp {} = []
getOpTupleDesc InsertOp {} = []
getOpTupleDesc op = tupleDesc op
