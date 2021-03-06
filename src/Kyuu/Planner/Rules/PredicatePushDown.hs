{-# LANGUAGE GADTs #-}

module Kyuu.Planner.Rules.PredicatePushDown (predicatePushDownOptimize) where

import Control.Monad
import Data.List (find)
import Data.Maybe (isJust)
import Data.Set (Set)
import qualified Data.Set as Set
import Kyuu.Core
import Kyuu.Expression
import Kyuu.Planner.DataSource
import qualified Kyuu.Planner.LogicalPlan as L
import Kyuu.Prelude
import Kyuu.Value

predicatePushDownOptimize ::
  (StorageBackend m) => L.LogicalPlan -> Kyuu m L.LogicalPlan
predicatePushDownOptimize lp = do
  (pp, _) <- predicatePushDown lp []
  return pp

predicatePushDown ::
  (StorageBackend m) =>
  L.LogicalPlan ->
  [SqlExpr Value] ->
  Kyuu m (L.LogicalPlan, [SqlExpr Value])
predicatePushDown lp@(L.Projection exprs schema child) preds = do
  (newChild, _) <- predicatePushDown child preds
  return (lp {L.childPlan = newChild}, [])
predicatePushDown lp@(L.Selection conds schema child) preds = do
  (newChild, newPreds) <- predicatePushDown child (preds ++ conds)
  case newPreds of
    [] -> return (newChild, [])
    newConds ->
      return
        ( lp
            { L.childPlan = newChild,
              L.conditions = newConds
            },
          []
        )
predicatePushDown lp@(L.Aggregation _ _ _ child) preds = do
  (newChild, _) <- predicatePushDown child []
  return (lp {L.childPlan = newChild}, preds)
predicatePushDown lp@(L.DataSource tableId tableName sArgs indexPaths schema) preds = do
  let (pushed, rejected) = groupWith isSargableExpr preds [] []
      newSearchArgs = sArgs ++ pushed
  return (lp {L.searchArgs = newSearchArgs}, rejected)
  where
    groupWith f [] accT accF = (accT, accF)
    groupWith f (x : xs) accT accF =
      if f x
        then groupWith f xs (x : accT) accF
        else groupWith f xs accT (x : accF)
predicatePushDown lp@(L.Join _ joinQuals otherQuals _ left right) preds = do
  let ps = joinQuals ++ otherQuals ++ preds
      (joinQuals', leftQuals, rightQuals, otherQuals') =
        groupJoinQuals ps left right [] [] [] []
      lpQuals =
        lp
          { L.joinQuals = joinQuals',
            L.otherQuals = otherQuals'
          }

  (newLeft, leftPreds) <- predicatePushDown left leftQuals
  (newRight, rightPreds) <- predicatePushDown right rightQuals

  return
    ( lpQuals {L.leftChild = newLeft, L.rightChild = newRight},
      leftPreds ++ rightPreds
    )
  where
    hasColumn :: OID -> OID -> TupleDesc -> Bool
    hasColumn tId cId =
      isJust
        . find
          ( \(ColumnDesc tId' cId') ->
              tId' == tId && cId' == cId
          )

    groupJoinQuals ::
      [SqlExpr Value] ->
      L.LogicalPlan ->
      L.LogicalPlan ->
      [SqlExpr Value] ->
      [SqlExpr Value] ->
      [SqlExpr Value] ->
      [SqlExpr Value] ->
      ( [SqlExpr Value],
        [SqlExpr Value],
        [SqlExpr Value],
        [SqlExpr Value]
      )
    groupJoinQuals [] left right accEq accLeft accRight accOther =
      (accEq, accLeft, accRight, accOther)
    groupJoinQuals (p@(BinOpExpr BEqual lExpr@(ColumnRefExpr ltId lcId) rExpr@(ColumnRefExpr rtId rcId)) : ps) left right accEq accLeft accRight accOther =
      let leftSchema = L.getLogicalTupleDesc left
          rightSchema = L.getLogicalTupleDesc right
       in case ( hasColumn ltId lcId leftSchema,
                 hasColumn rtId rcId rightSchema
               ) of
            (True, True) ->
              groupJoinQuals ps left right (p : accEq) accLeft accRight accOther
            _ ->
              case (hasColumn rtId rcId leftSchema, hasColumn ltId lcId rightSchema) of
                (True, True) ->
                  groupJoinQuals
                    ps
                    left
                    right
                    (BinOpExpr BEqual rExpr lExpr : accEq)
                    accLeft
                    accRight
                    accOther
                _ ->
                  groupJoinQuals
                    ps
                    left
                    right
                    accEq
                    accLeft
                    accRight
                    (p : accOther)
    groupJoinQuals (p : ps) left right accEq accLeft accRight accOther =
      let leftSchema = L.getLogicalTupleDesc left
          rightSchema = L.getLogicalTupleDesc right
          columns = traverseExpr extractColumn p
       in if all (\(tId, cId) -> hasColumn tId cId leftSchema) columns
            then groupJoinQuals ps left right accEq (p : accLeft) accRight accOther
            else
              if all (\(tId, cId) -> hasColumn tId cId rightSchema) columns
                then groupJoinQuals ps left right accEq accLeft (p : accRight) accOther
                else groupJoinQuals ps left right accEq accLeft accRight (p : accOther)

    extractColumn :: SqlExpr Value -> Set (OID, OID)
    extractColumn (ColumnRefExpr tableId colId) = Set.singleton (tableId, colId)
    extractColumn _ = mempty
predicatePushDown lp@(L.Offset _ _ child) preds = do
  (newChild, newPreds) <- predicatePushDown child preds
  return (lp {L.childPlan = newChild}, newPreds)
predicatePushDown lp@(L.Limit _ _ child) preds = do
  (newChild, newPreds) <- predicatePushDown child preds
  return (lp {L.childPlan = newChild}, newPreds)
