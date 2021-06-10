module Kyuu.Executor.Accumulator
  ( Accumulator,
    getAccumulator,
    accumulateValues,
    aggregateValue,
  )
where

import Kyuu.Expression
import Kyuu.Parse.Analyzer
import Kyuu.Prelude
import Kyuu.Value

data Accumulator
  = Count Int
  | Max Value
  | Min Value
  | Sum Value
  deriving (Show)

getAccumulator :: AggregateDesc -> Accumulator
getAccumulator (AggregateDesc AggCount _) = Count 0
getAccumulator (AggregateDesc AggMax _) = Max VNull
getAccumulator (AggregateDesc AggMin _) = Min VNull
getAccumulator (AggregateDesc AggSum _) = Sum VNull

accumulateValues :: [Value] -> Accumulator -> Accumulator
accumulateValues _ (Count count) = Count (count + 1)
accumulateValues [val] (Max VNull) = Max val
accumulateValues [val] acc@(Max v) = case evalBinOpExpr BGreaterThan val v of
  VBool True -> Max val
  _ -> acc
accumulateValues [val] (Min VNull) = Min val
accumulateValues [val] acc@(Min v) = case evalBinOpExpr BLessThan val v of
  VBool True -> Min val
  _ -> acc
accumulateValues [val] (Sum VNull) = Sum val
accumulateValues [val] (Sum v) = Sum $ evalBinOpExpr BAdd val v

aggregateValue :: Accumulator -> Value
aggregateValue (Count count) = VInt count
aggregateValue (Max val) = val
aggregateValue (Min val) = val
aggregateValue (Sum val) = val
