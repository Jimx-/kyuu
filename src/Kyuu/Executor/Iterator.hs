{-# LANGUAGE NamedFieldPuns #-}

module Kyuu.Executor.Iterator
  ( nextTuple,
  )
where

import Control.Lens
import Control.Monad.State.Lazy
import Data.List (find, intercalate, intersperse)
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

rescan :: (StorageBackend m) => Operator m -> Kyuu m (Operator m)
rescan op@(TableScanOp tableId _ _ _) = return op
rescan op = return op

nextTuple ::
  (StorageBackend m) => Operator m -> Kyuu m (Maybe Tuple, Operator m)
nextTuple op@(TableScanOp tableId filters tupleDesc scanIterator) =
  case scanIterator of
    (Just scanIterator) -> do
      (newIterator, tuple) <-
        tableScanNext
          scanIterator
          Forward
      let newOp = op {scanIterator = Just newIterator}
      case tuple of
        Nothing -> return (Nothing, newOp)
        Just tuple -> do
          res <- forM filters $
            \expr -> evalExpr expr tuple
          let accept =
                foldr
                  (evalBinOpExpr BAnd)
                  (VBool True)
                  res
          case accept of
            (VBool True) ->
              return
                ( Just tuple,
                  newOp
                )
            _ -> nextTuple newOp
    _ -> lerror (InvalidState "scan operator is not open")
nextTuple op@(IndexScanOp tableId indexId filters _ tupleDesc indexScanIterator) =
  case indexScanIterator of
    (Just indexScanIterator) -> do
      (newIterator, tuple) <-
        indexScanNext
          indexScanIterator
          Forward
      let newOp = op {indexScanIterator = Just newIterator}
      case tuple of
        Nothing -> return (Nothing, newOp)
        Just tuple -> do
          res <- forM filters $
            \expr -> evalExpr expr tuple
          let accept =
                foldr
                  (evalBinOpExpr BAnd)
                  (VBool True)
                  res
          case accept of
            (VBool True) ->
              return
                ( Just tuple,
                  newOp
                )
            _ -> nextTuple newOp
    _ -> lerror (InvalidState "index scan operator is not open")
nextTuple op@(SelectionOp exprs tupleDesc input) = do
  (inputTuple, newInput) <- nextTuple input
  let newOp = op {input = newInput}

  case inputTuple of
    Just tuple -> do
      res <- forM exprs $ \expr -> evalExpr expr tuple
      let accept =
            foldr
              (evalBinOpExpr BAnd)
              (VBool True)
              res
      case accept of
        (VBool True) -> return (Just tuple, newOp)
        _ -> nextTuple newOp
    _ -> return (Nothing, newOp)
nextTuple (ProjectionOp columns tupleDesc input) = do
  (inputTuple, newInput) <- nextTuple input

  newTuple <- case inputTuple of
    Just tuple -> do
      projectedValues <- forM columns $ \expr -> do
        value <- evalExpr expr tuple
        return $
          Tuple
            [ ColumnDesc
                (getOutputTableId expr)
                (getOutputColumnId expr)
            ]
            [value]

      return $
        Just
          ( sortTuple
              tupleDesc
              (mconcat projectedValues)
          )
    _ -> return Nothing

  return (newTuple, ProjectionOp columns tupleDesc newInput)
nextTuple op@(AggregationOp _ _ (tuple : tuples) _ _) = return (Just tuple, op {tuples = tuples})
nextTuple op@(AggregationOp _ _ [] _ _) = return (Nothing, op)
nextTuple op@(NestLoopOp joinQuals [] tupleDesc outerInput innerInput) = do
  (outerTuple, newOuter) <- nextTuple outerInput

  case outerTuple of
    Just outerTuple -> do
      innerOp <- rescan innerInput
      (tuples, newInner) <-
        fetchInnerTuples
          outerTuple
          joinQuals
          innerOp
          []

      nextTuple op {overflow = tuples, outerInput = newOuter}
    Nothing -> return (Nothing, op {outerInput = newOuter})
  where
    fetchInnerTuples ::
      (StorageBackend m) =>
      Tuple ->
      [SqlExpr Value] ->
      Operator m ->
      [Tuple] ->
      Kyuu m ([Tuple], Operator m)
    fetchInnerTuples outerTuple joinQuals innerInput tuples = do
      (innerTuple, newInner) <- nextTuple innerInput

      case innerTuple of
        Just innerTuple -> do
          let tuple = outerTuple <> innerTuple

          res <- forM joinQuals $
            \expr -> evalExpr expr tuple
          let accept =
                foldr
                  (evalBinOpExpr BAnd)
                  (VBool True)
                  res
              newTuples = case accept of
                (VBool True) -> tuple : tuples
                _ -> tuples
          fetchInnerTuples
            outerTuple
            joinQuals
            newInner
            newTuples
        Nothing -> return (tuples, newInner)
nextTuple op@(NestLoopOp _ (tuple : tuples) tupleDesc _ _) = do
  return
    ( Just $ sortTuple tupleDesc tuple,
      op {overflow = tuples}
    )
nextTuple op@(HashJoinOp outerKeys _ ht [] tupleDesc outerInput _) = do
  (outerTuple, newOuter) <- nextTuple outerInput

  case outerTuple of
    Just outerTuple -> do
      keys <- forM outerKeys $ \expr -> evalExpr expr outerTuple
      let tuples = map (outerTuple <>) (lookupHashTable keys ht)
      nextTuple op {overflow = tuples, outerInput = newOuter}
    Nothing -> return (Nothing, op {outerInput = newOuter})
nextTuple op@(HashJoinOp _ _ _ (tuple : tuples) tupleDesc _ _) =
  return
    ( Just $ sortTuple tupleDesc tuple,
      op {overflow = tuples}
    )
nextTuple op@(LimitOp 0 _ _) = return (Nothing, op)
nextTuple op@(LimitOp lim _ input) = do
  (tuple, newInput) <- nextTuple input

  case tuple of
    Just tuple -> return (Just tuple, op {limit = lim - 1, input = newInput})
    _ -> return (Nothing, op {limit = 0, input = newInput})
nextTuple op@(OffsetOp 0 _ input) = do
  (tuple, newInput) <- nextTuple input
  return (tuple, op {input = newInput})
nextTuple op@(OffsetOp offset _ input) = do
  (tuple, newInput) <- nextTuple input

  case tuple of
    Just _ -> nextTuple op {offset = offset - 1, input = newInput}
    _ -> return (Nothing, op {input = newInput})
nextTuple op@(CreateTableOp schema done) =
  if done
    then return (Nothing, op)
    else do
      createTableWithCatalog schema
      return (Nothing, op {done = True})
nextTuple op@(InsertOp tableId targetExprs) = do
  schema <- lookupTableById tableId
  table <- openTable tableId

  case (schema, table) of
    (Just schema, Just table) -> do
      insertRows tableId schema table targetExprs
      return (Nothing, op)
    _ -> lerror (TableNotFound tableId)
  where
    insertRows tableId TableSchema {tableCols} table targetExprs =
      forM_ targetExprs $ \exprs -> do
        tuple <- mconcat <$> mapM (`evalExpr` mempty) exprs
        let filledTuple =
              sortTuple
                ( map
                    ( \ColumnSchema {colTable, colId} ->
                        ColumnDesc
                          colTable
                          colId
                    )
                    tableCols
                )
                (fillTuple tableCols tuple)
        insertTuple table filledTuple

    fillTuple [] tuple = tuple
    fillTuple (ColumnSchema {colTable, colId, colType} : cols) tuple@(Tuple tupleDesc _) =
      case find
        ( \(ColumnDesc tableId' colId') ->
            tableId
              == tableId'
              && colId
              == colId'
        )
        tupleDesc of
        (Just _) -> fillTuple cols tuple
        _ ->
          Tuple [ColumnDesc tableId colId] [VNull]
            <> fillTuple cols tuple
nextTuple (PrintOp printHeader tupleDesc input) = do
  (inputTuple, newInput) <- nextTuple input

  newTuple <- case inputTuple of
    Just tuple@(Tuple _ vs) -> do
      when printHeader $ do
        headers <- forM tupleDesc $
          \(ColumnDesc tableId colId) -> do
            schema <-
              lookupTableColumnById
                tableId
                colId
            case schema of
              Nothing -> return "(null)"
              (Just (ColumnSchema _ _ name _)) ->
                return name
        liftIO $ putStrLn $ intercalate "|" headers

      liftIO $ putStrLn $ intercalate "|" $ map show vs

      return $ Just tuple
    _ -> return Nothing

  return (newTuple, PrintOp False tupleDesc newInput)
