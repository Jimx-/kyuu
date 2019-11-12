{-# LANGUAGE TemplateHaskell, NamedFieldPuns #-}
module Kyuu.Executor.Executor
        ( executePlan
        , nextTuple
        )
where

import           Control.Monad.State.Lazy
import           Control.Lens

import           Data.List                      ( find )
import           Data.Map                       ( Map )
import qualified Data.Map                      as Map
import           Data.Maybe

import           Kyuu.Core
import           Kyuu.Catalog.Catalog
import           Kyuu.Catalog.Schema
import           Kyuu.Error
import           Kyuu.Expression
import           Kyuu.Prelude
import           Kyuu.Value
import           Kyuu.Executor.Operators
import           Kyuu.Executor.Builder
import           Kyuu.Storage.Backend

executePlan :: (StorageBackend m) => ExecutionPlan m -> Kyuu m ()
executePlan (ExecutionPlan plan) = do
        op <- open plan
        drain op

drain :: (StorageBackend m) => Operator m -> Kyuu m ()
drain op = do
        (tuple, newOp) <- nextTuple op

        case tuple of
                Just tuple -> drain newOp
                _          -> return ()

rescan :: (StorageBackend m) => Operator m -> Kyuu m (Operator m)
rescan op@(TableScanOp tableId _ _ _ _) = return op
rescan op                               = return op

open :: (StorageBackend m) => Operator m -> Kyuu m (Operator m)
open op@(TableScanOp tableId _ filters tupleDesc _) = do
        schema <- lookupTableById tableId
        table  <- openTable tableId
        case (schema, table) of
                (Just schema, Just table) -> do
                        iterator <- beginTableScan table
                        return $ TableScanOp tableId
                                             (Just schema)
                                             filters
                                             tupleDesc
                                             (Just iterator)
                _ -> lerror (TableNotFound tableId)
open op@SelectionOp { input = input } = do
        newInput <- open input
        return op { input = newInput }
open op@ProjectionOp { input = input } = do
        newInput <- open input
        return op { input = newInput }
open op@NestLoopOp { outerInput = outerInput, innerInput = innerInput } = do
        newOuter <- open outerInput
        newInner <- open innerInput
        return op { outerInput = newOuter, innerInput = newInner }
open op@PrintOp { input = input } = do
        newInput <- open input
        return op { input = newInput }
open op = return op

nextTuple
        :: (StorageBackend m) => Operator m -> Kyuu m (Maybe Tuple, Operator m)
nextTuple op@(TableScanOp tableId schema filters tupleDesc scanIterator) = do
        case (schema, scanIterator) of
                (Just schema, Just scanIterator) -> do
                        tableTuple <- tableScanNext scanIterator Forward
                        case tableTuple of
                                Nothing         -> return (Nothing, op)
                                Just tableTuple -> do
                                        tupleBuf <- getTupleData tableTuple
                                        let     tuple = fromJust $ decodeTuple
                                                        tupleBuf
                                                        schema
                                                newOp = TableScanOp
                                                        tableId
                                                        (Just schema)
                                                        filters
                                                        tupleDesc
                                                        (Just scanIterator)
                                        res <- forM filters
                                                $ \expr -> evalExpr expr tuple
                                        let
                                                accept = foldr
                                                        (evalBinOpExpr BAnd)
                                                        (VBool True)
                                                        res
                                        case accept of
                                                (VBool True) ->
                                                        return
                                                                ( Just tuple
                                                                , newOp
                                                                )
                                                _ -> nextTuple newOp
                _ -> lerror (InvalidState "scan operator is not open")


nextTuple (SelectionOp exprs tupleDesc input) = do
        (inputTuple, newInput) <- nextTuple input
        let newOp = SelectionOp exprs tupleDesc newInput

        case inputTuple of
                Just tuple -> do
                        res <- forM exprs $ \expr -> evalExpr expr tuple
                        let
                                accept = foldr (evalBinOpExpr BAnd)
                                               (VBool True)
                                               res
                        case accept of
                                (VBool True) -> return (Just tuple, newOp)
                                _            -> nextTuple newOp
                _ -> return (Nothing, newOp)

nextTuple (ProjectionOp columns tupleDesc input) = do
        (inputTuple, newInput) <- nextTuple input

        newTuple               <- case inputTuple of
                Just tuple -> do
                        projectedValues <- forM columns $ \expr -> do
                                value <- evalExpr expr tuple
                                return $ Tuple
                                        [ ColumnDesc
                                                  (getOutputTableId expr)
                                                  (getOutputColumnId expr)
                                        ]
                                        [value]

                        return
                                $ Just
                                          (sortTuple
                                                  tupleDesc
                                                  (mconcat projectedValues)
                                          )
                _ -> return Nothing

        return (newTuple, ProjectionOp columns tupleDesc newInput)

nextTuple op@(NestLoopOp joinQuals tupleDesc outerInput innerInput) = do
        (outerTuple, newOuter) <- nextTuple outerInput

        case outerTuple of
                Just outerTuple -> do
                        innerOp           <- rescan innerInput
                        (tuple, newInner) <- fetchInnerTuple outerTuple
                                                             joinQuals
                                                             innerOp

                        case tuple of
                                Just tuple -> return
                                        ( Just $ sortTuple tupleDesc tuple
                                        , op { outerInput = newOuter
                                             , innerInput = newInner
                                             }
                                        )
                                Nothing ->
                                        nextTuple op { outerInput = newOuter }

                Nothing -> return (Nothing, op { outerInput = newOuter })

    where
        fetchInnerTuple
                :: (StorageBackend m)
                => Tuple
                -> [SqlExpr Value]
                -> Operator m
                -> Kyuu m (Maybe Tuple, Operator m)
        fetchInnerTuple outerTuple joinQuals innerInput = do
                (innerTuple, newInner) <- nextTuple innerInput

                case innerTuple of
                        Just innerTuple -> do
                                let tuple = outerTuple <> innerTuple

                                res <- forM joinQuals
                                        $ \expr -> evalExpr expr tuple
                                let
                                        accept = foldr
                                                (evalBinOpExpr BAnd)
                                                (VBool True)
                                                res
                                case accept of
                                        (VBool True) ->
                                                return (Just tuple, newInner)
                                        _ -> fetchInnerTuple outerTuple
                                                             joinQuals
                                                             newInner

                        Nothing -> fetchInnerTuple outerTuple
                                                   joinQuals
                                                   newInner

nextTuple op@(CreateTableOp schema done) = if done
        then return (Nothing, op)
        else do
                createTableWithCatalog schema
                return (Nothing, op { done = True })

nextTuple op@(InsertOp tableId targetExprs) = do
        schema <- lookupTableById tableId
        table  <- openTable tableId

        case (schema, table) of
                (Just schema, Just table) -> do
                        insertRows tableId schema table targetExprs
                        return (Nothing, op)
                _ -> lerror (TableNotFound tableId)
    where
        insertRows tableId TableSchema { tableCols } table targetExprs =
                forM_ targetExprs $ \exprs -> do
                        tuple <- mconcat <$> mapM (`evalExpr` mempty) exprs
                        let     filledTuple = sortTuple
                                        (map
                                                (\ColumnSchema { colTable, colId } ->
                                                        ColumnDesc
                                                                colTable
                                                                colId
                                                )
                                                tableCols
                                        )
                                        (fillTuple tableCols tuple)
                                tupleBuf = encodeTuple filledTuple
                        insertTuple table tupleBuf

        fillTuple [] tuple = tuple
        fillTuple (ColumnSchema { colTable, colId, colType } : cols) tuple@(Tuple tupleDesc _)
                = case
                                find
                                        (\(ColumnDesc tableId' colId') ->
                                                tableId
                                                        == tableId'
                                                        && colId
                                                        == colId'
                                        )
                                        tupleDesc
                        of
                                (Just _) -> fillTuple cols tuple
                                _ ->
                                        Tuple [ColumnDesc tableId colId] [VNull]
                                                <> fillTuple cols tuple

nextTuple (PrintOp printHeader tupleDesc input) = do
        (inputTuple, newInput) <- nextTuple input

        newTuple               <- case inputTuple of
                Just tuple@(Tuple _ vs) -> do
                        when printHeader $ do
                                forM_ tupleDesc
                                        $ \(ColumnDesc tableId colId) -> do
                                                  schema <-
                                                          lookupTableColumnById
                                                                  tableId
                                                                  colId
                                                  case schema of
                                                          Nothing ->
                                                                  liftIO
                                                                          $ putStr
                                                                                    "(null)\t"
                                                          (Just (ColumnSchema _ _ name _))
                                                                  -> liftIO
                                                                          $ putStr
                                                                          $ name
                                                                          ++ "\t"
                                liftIO $ putStrLn " "

                        forM_ vs $ \val -> liftIO $ putStr $ show val ++ "\t"
                        liftIO $ putStrLn " "

                        return $ Just tuple

                _ -> return Nothing

        return (newTuple, PrintOp False tupleDesc newInput)
