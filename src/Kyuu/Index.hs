{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NamedFieldPuns #-}

module Kyuu.Index
  ( Index (..),
    IndexScanIterator (..),
    ScanOperator (..),
    ScanKey (..),
    insertIndex,
    beginIndexScan,
    rescanIndex,
    indexScanNext,
    endIndexScan,
    module X,
  )
where

import qualified Data.ByteString as B
import Data.List
import Kyuu.Catalog.Schema
import Kyuu.Core
import Kyuu.Error
import Kyuu.Expression
import Kyuu.Prelude
import Kyuu.Storage.Backend as X
  ( ScanDirection (..),
  )
import qualified Kyuu.Storage.Backend as S
import Kyuu.Table
import Kyuu.Value

data Index m = Index
  { indexId :: OID,
    indexSchema :: IndexSchema,
    indexStorage :: S.IndexType m
  }

data IndexScanIterator m = IndexScanIterator
  { index :: Index m,
    table :: Table m,
    iterator :: S.IndexScanIteratorType m
  }

data ScanOperator
  = SEqual
  | SLess
  | SGreater
  | SLessEqual
  | SGreaterEqual
  deriving (Eq, Show)

data ScanKey = ScanKey Int ScanOperator Value
  deriving (Show)

getExprOperator :: ScanOperator -> BinOp
getExprOperator SEqual = BEqual
getExprOperator SLess = BLessThan
getExprOperator SGreater = BGreaterThan

getIndexScanPredicate :: TupleDesc -> [ScanKey] -> B.ByteString -> Maybe Bool
getIndexScanPredicate tupleDesc scanKeys buf =
  case decodeTupleWithDesc buf tupleDesc of
    Nothing -> Nothing
    (Just tuple) -> Just $ checkScanKeys scanKeys tuple
  where
    getColValue colNum (Tuple [] _) = VNull
    getColValue colNum (Tuple ((ColumnDesc _ colNum') : tds) (v : vds)) =
      if colNum == colNum'
        then v
        else getColValue colNum (Tuple tds vds)

    checkScanKeys [] _ = True
    checkScanKeys (ScanKey colNum op value : xs) tuple =
      let colValue = getColValue colNum tuple
          res = evalBinOpExpr (getExprOperator op) colValue value
       in case res of
            (VBool True) -> checkScanKeys xs tuple
            (VBool False) -> False

insertIndex ::
  (StorageBackend m) => Index m -> Tuple -> TupleSlot m -> Kyuu m ()
insertIndex Index {indexStorage} key tupleSlot = do
  let keyBuf = encodeTuple key
  S.insertIndex indexStorage keyBuf tupleSlot

beginIndexScan ::
  (StorageBackend m) =>
  Index m ->
  Table m ->
  Kyuu m (IndexScanIterator m)
beginIndexScan index@Index {indexStorage} table@Table {tableStorage} = do
  txn <- getCurrentTransaction
  iterator <- S.beginIndexScan txn indexStorage tableStorage
  return $ IndexScanIterator index table iterator

rescanIndex ::
  (StorageBackend m) =>
  IndexScanIterator m ->
  [ScanKey] ->
  ScanDirection ->
  Kyuu m (IndexScanIterator m)
rescanIndex IndexScanIterator {index = index@Index {indexSchema = IndexSchema {colNums}}, table = table@Table {tableId}, iterator} scanKeys dir =
  do
    let tupleDesc = fmap (ColumnDesc tableId) colNums
        boundaryKeys = getBoundaryKeys scanKeys dir
        startKey = getStartKey tupleDesc boundaryKeys
    newIt <-
      S.rescanIndex
        iterator
        startKey
        (getIndexScanPredicate tupleDesc scanKeys)
    return $ IndexScanIterator index table iterator
  where
    getBoundaryKeys [] _ = []
    getBoundaryKeys (s@(ScanKey _ op _) : xs) Forward = case op of
      SEqual -> s : getBoundaryKeys xs Forward
      SGreater -> s : getBoundaryKeys xs Forward
      SGreaterEqual -> s : getBoundaryKeys xs Forward
      _ -> getBoundaryKeys xs Forward
    getBoundaryKeys (s@(ScanKey _ op _) : xs) Backward = case op of
      SEqual -> s : getBoundaryKeys xs Backward
      SLess -> s : getBoundaryKeys xs Backward
      SLessEqual -> s : getBoundaryKeys xs Backward
      _ -> getBoundaryKeys xs Backward

    getCol colDesc [] = Tuple [colDesc] [VNull]
    getCol colDesc@(ColumnDesc tableId colNum) (ScanKey colNum' _ value : xs) =
      if colNum == colNum'
        then Tuple [colDesc] [value]
        else getCol colDesc xs

    getStartKey _ [] = Nothing
    getStartKey tupleDesc boundaryKeys =
      Just $
        encodeTuple $
          mconcat $
            fmap
              (`getCol` boundaryKeys)
              tupleDesc

indexScanNext ::
  (StorageBackend m) =>
  IndexScanIterator m ->
  ScanDirection ->
  Kyuu m (IndexScanIterator m, Maybe Tuple)
indexScanNext IndexScanIterator {index, table = table@Table {tableSchema}, iterator} dir =
  do
    (newIterator, tableTuple) <- S.indexScanNext iterator dir
    let newIt = IndexScanIterator index table newIterator
    case tableTuple of
      Nothing -> return (newIt, Nothing)
      (Just tableTuple) -> do
        tupleBuf <- S.getTupleData tableTuple
        case decodeTuple tupleBuf tableSchema of
          (Just tuple) ->
            return (newIt, Just tuple)
          Nothing ->
            lerror
              ( DataCorrupted
                  "cannot decode tuple"
              )

endIndexScan ::
  (StorageBackend m) =>
  IndexScanIterator m ->
  Kyuu m (IndexScanIterator m)
endIndexScan IndexScanIterator {index, table, iterator} = do
  newIt <- S.endIndexScan iterator
  return $ IndexScanIterator index table newIt

formIndexKeyTuple :: (StorageBackend m) => Index m -> Tuple -> Kyuu m Tuple
formIndexKeyTuple Index {indexSchema = IndexSchema {colNums}} (Tuple tupleDesc vals) =
  fmap mconcat $
    forM colNums $ \colNum ->
      case find
        ( \(ColumnDesc _ colId, _) ->
            colNum == colId
        )
        (zip tupleDesc vals) of
        (Just (colDesc, val)) ->
          return $ Tuple [colDesc] [val]
        _ -> lerror (ColumnNumNotFound colNum)
