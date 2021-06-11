{-# LANGUAGE DuplicateRecordFields #-}

module Kyuu.Executor.Operators
  ( Operator (..),
    HashTable,
    getOpTupleDesc,
    mkAggregationOp,
    mkNestLoopJoinOp,
    mkHashJoinOp,
    emptyHashTable,
    insertHashTable,
    lookupHashTable,
  )
where

import Data.Hashable
import qualified Data.MultiMap as MM
import Kyuu.Catalog.Catalog
import Kyuu.Catalog.Schema
import Kyuu.Core
import Kyuu.Expression
import Kyuu.Index
import Kyuu.Parse.Analyzer
import Kyuu.Prelude
import Kyuu.Table
import Kyuu.Value

type HashTable = MM.MultiMap Int ([Value], Tuple)

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
  | AggregationOp
      { aggregates :: [AggregateDesc],
        groupBys :: [SqlExpr Value],
        tuples :: [Tuple],
        tupleDesc :: TupleDesc,
        input :: Operator m
      }
  | NestLoopOp
      { joinQuals :: [SqlExpr Value],
        overflow :: [Tuple],
        tupleDesc :: TupleDesc,
        outerInput :: Operator m,
        innerInput :: Operator m
      }
  | HashJoinOp
      { outerKeys :: [SqlExpr Value],
        innerKeys :: [SqlExpr Value],
        hashTable :: HashTable,
        overflow :: [Tuple],
        tupleDesc :: TupleDesc,
        outerInput :: Operator m,
        innerInput :: Operator m
      }
  | OffsetOp
      { offset :: Int,
        tupleDesc :: TupleDesc,
        input :: Operator m
      }
  | LimitOp
      { limit :: Int,
        tupleDesc :: TupleDesc,
        input :: Operator m
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
  show (AggregationOp aggs groupBys _ tupleDesc input) =
    "AggregationOp {aggregates = "
      ++ show aggs
      ++ ", groupBys = "
      ++ show groupBys
      ++ ", tupleDesc = "
      ++ show tupleDesc
      ++ ", input = "
      ++ show input
      ++ "}"
  show (NestLoopOp joinQuals _ tupleDesc outerInput innerInput) =
    "NestLoopOp {joinQuals = "
      ++ show joinQuals
      ++ ", tupleDesc = "
      ++ show tupleDesc
      ++ ", outerInput = "
      ++ show outerInput
      ++ ", innerInput = "
      ++ show innerInput
      ++ "}"
  show (HashJoinOp outerKeys innerKeys _ _ tupleDesc outerInput innerInput) =
    "HashJoinOp {outerKeys = "
      ++ show outerKeys
      ++ ", innerKeys = "
      ++ show innerKeys
      ++ ", tupleDesc = "
      ++ show tupleDesc
      ++ ", outerInput = "
      ++ show outerInput
      ++ ", innerInput = "
      ++ show innerInput
      ++ "}"
  show (OffsetOp offset tupleDesc input) =
    "OffsetOp {offset = "
      ++ show offset
      ++ ", tupleDesc = "
      ++ show tupleDesc
      ++ ", input = "
      ++ show input
      ++ "}"
  show (LimitOp limit tupleDesc input) =
    "LimitOp {limit = "
      ++ show limit
      ++ ", tupleDesc = "
      ++ show tupleDesc
      ++ ", input = "
      ++ show input
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

mkAggregationOp :: (StorageBackend m) => [AggregateDesc] -> [SqlExpr Value] -> TupleDesc -> Operator m -> Operator m
mkAggregationOp aggs groupBys = AggregationOp aggs groupBys []

mkNestLoopJoinOp :: (StorageBackend m) => [SqlExpr Value] -> TupleDesc -> Operator m -> Operator m -> Operator m
mkNestLoopJoinOp joinQuals = NestLoopOp joinQuals []

mkHashJoinOp :: (StorageBackend m) => [SqlExpr Value] -> [SqlExpr Value] -> TupleDesc -> Operator m -> Operator m -> Operator m
mkHashJoinOp outerKeys innerKeys = HashJoinOp outerKeys innerKeys MM.empty []

emptyHashTable :: HashTable
emptyHashTable = MM.empty

insertHashTable :: [Value] -> Tuple -> HashTable -> HashTable
insertHashTable vals tuple = MM.insert (hash vals) (vals, tuple)

lookupHashTable :: [Value] -> HashTable -> [Tuple]
lookupHashTable vals ht =
  let tuples = MM.lookup (hash vals) ht
   in [tuple | (v, tuple) <- tuples, v == vals]
