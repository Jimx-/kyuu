{-# LANGUAGE GADTs, StandaloneDeriving #-}
module Kyuu.Expression
        ( BinOp(..)
        , SqlExpr(..)
        , getBinOp
        , getOutputTableId
        , getOutputColumnId
        , isSargableExpr
        , evalExpr
        , evalBinOpExpr
        )
where

import           Kyuu.Core
import           Kyuu.Prelude
import           Kyuu.Value

data BinOp = BAdd
           | BAnd
           | BEqual
           | BLessThan
           | BGreaterThan
           deriving (Eq, Show)

data SqlExpr t where
  ColumnRefExpr ::OID -> OID -> SqlExpr Value
  ColumnAssignExpr ::OID -> OID -> SqlExpr Value -> SqlExpr Tuple
  BinOpExpr ::BinOp -> SqlExpr Value -> SqlExpr Value -> SqlExpr Value
  ValueExpr ::Value -> SqlExpr Value

deriving instance (Eq t) => Eq (SqlExpr t)
deriving instance (Show t) => Show (SqlExpr t)

getBinOp :: String -> BinOp
getBinOp "+"   = BAdd
getBinOp "and" = BAnd
getBinOp "="   = BEqual
getBinOp "<"   = BLessThan
getBinOp ">"   = BGreaterThan

getOutputTableId :: SqlExpr Value -> OID
getOutputTableId (ColumnRefExpr tId _) = tId
getOutputTableId _                     = 0

getOutputColumnId :: SqlExpr Value -> OID
getOutputColumnId (ColumnRefExpr _ cId) = cId
getOutputColumnId _                     = 0

isSargableExpr :: SqlExpr t -> Bool
isSargableExpr (BinOpExpr op left right) =
        isSargableOp op && isSargableExpr left && isSargableExpr right
        where isSargableOp _ = True

isSargableExpr (ColumnRefExpr _ _) = True
isSargableExpr (ValueExpr _      ) = True

evalBinOpExpr :: BinOp -> Value -> Value -> Value
evalBinOpExpr BAdd         (VDouble l) (VDouble r) = VDouble (l + r)
evalBinOpExpr BAnd         (VBool   l) (VBool   r) = VBool (l && r)
evalBinOpExpr BEqual       (VInt    l) (VInt    r) = VBool (l == r)
evalBinOpExpr BLessThan    (VInt    l) (VInt    r) = VBool (l < r)
evalBinOpExpr BLessThan    (VDouble l) (VDouble r) = VBool (l < r)
evalBinOpExpr BGreaterThan (VInt    l) (VInt    r) = VBool (l > r)
evalBinOpExpr BGreaterThan (VDouble l) (VDouble r) = VBool (l > r)

evalExpr :: (StorageBackend m) => SqlExpr t -> Tuple -> Kyuu m t
evalExpr (ColumnRefExpr tableId colId) (Tuple [] _) = return VNull
evalExpr expr@(ColumnRefExpr tableId colId) (Tuple ((ColumnDesc tableId' colId') : tds) (v : vds))
        = if tableId == tableId' && colId == colId'
                then return v
                else evalExpr expr (Tuple tds vds)

evalExpr (ColumnAssignExpr tableId colId expr) tuple = do
        val <- evalExpr expr tuple
        return $ Tuple [ColumnDesc tableId colId] [val]

evalExpr (BinOpExpr op left right) tuple = do
        lval <- evalExpr left tuple
        rval <- evalExpr right tuple
        return $ evalBinOpExpr op lval rval

evalExpr (ValueExpr v) _ = return v
