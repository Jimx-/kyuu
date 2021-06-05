{-# LANGUAGE GADTs #-}

module Kyuu.Executor.Nodes.IndexScan
  ( initIndexScanOp,
    closeIndexScanOp,
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
import Kyuu.Executor.Operators
import Kyuu.Expression
import Kyuu.Index
import Kyuu.Prelude
import Kyuu.Table
import Kyuu.Value

initIndexScanOp :: (StorageBackend m) => Operator m -> Kyuu m (Operator m)
initIndexScanOp op@(IndexScanOp tableId indexId _ indexQuals _ _) = do
  table <- openTable tableId
  index <- openIndex indexId
  case (table, index) of
    (Just table, Just index) -> do
      si <- beginIndexScan index table
      scanKeys <- buildScanKeys indexQuals
      si <- rescanIndex si scanKeys Forward
      return $ op {indexScanIterator = Just si}
    (Just _, Nothing) -> lerror (IndexNotFound indexId)
    (Nothing, _) -> lerror (TableNotFound tableId)

buildScanKeys :: (StorageBackend m) => [SqlExpr Value] -> Kyuu m [ScanKey]
buildScanKeys = mapM buildScanKey
  where
    buildScanKey :: (StorageBackend m) => SqlExpr Value -> Kyuu m ScanKey
    -- the expression has been normalized to the form (column op value) in buildIndexPaths
    buildScanKey (BinOpExpr op (ColumnRefExpr _ colNum) (ValueExpr val)) = return $ ScanKey colNum (getScanOperator op) val
    buildScanKey expr = lerror $ InvalidExpression $ "index qualifier " ++ show expr

closeIndexScanOp :: (StorageBackend m) => Operator m -> Kyuu m ()
closeIndexScanOp IndexScanOp {indexScanIterator = Just si} = void $ endIndexScan si
