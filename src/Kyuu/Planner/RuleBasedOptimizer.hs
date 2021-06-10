{-# LANGUAGE RecordWildCards #-}

module Kyuu.Planner.RuleBasedOptimizer
  ( optimizeLogicalPlan,
  )
where

import Control.Monad
import Data.List (find)
import Data.Maybe (catMaybes, isJust)
import Kyuu.Core
import Kyuu.Expression
import qualified Kyuu.Planner.LogicalPlan as L
import Kyuu.Planner.Rules.PredicatePushDown
import Kyuu.Prelude
import Kyuu.Value

whenMaybe :: Bool -> a -> Maybe a
whenMaybe True a = Just a
whenMaybe _ _ = Nothing

-- | Apply rule based optimization on logical plans
optimizeLogicalPlan ::
  (StorageBackend m) =>
  L.PlanBuilderState ->
  L.LogicalPlan ->
  Kyuu m L.LogicalPlan
optimizeLogicalPlan L.PlanBuilderState {..} lp = do
  let rules =
        catMaybes
          [ whenMaybe
              _needPredicatePushDown
              predicatePushDownOptimize
          ]
  foldM (flip ($)) lp rules
