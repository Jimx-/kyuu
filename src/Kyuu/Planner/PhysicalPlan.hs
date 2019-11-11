module Kyuu.Planner.PhysicalPlan
        ( PhysicalPlan(..)
        , getPhysicalPlan
        )
where

import           Kyuu.Core
import           Kyuu.Expression
import           Kyuu.Value
import           Kyuu.Prelude
import qualified Kyuu.Planner.LogicalPlan      as L
import           Kyuu.Catalog.Schema

data PhysicalPlan = TableScan { tableId :: OID
                              , tableName :: String
                              , filters :: [SqlExpr Value]
                              , tupleDesc :: TupleDesc }
                  | Selection { conds :: [SqlExpr Value]
                              , tupleDesc :: TupleDesc
                              , child :: PhysicalPlan }
                  | Projection { exprs :: [SqlExpr Value]
                               , tupleDesc :: TupleDesc
                               , child :: PhysicalPlan }
                  | NestedLoopJoin { joinType :: L.JoinType
                                   , joinQuals :: [SqlExpr Value]
                                   , otherQuals :: [SqlExpr Value]
                                   , tupleDesc :: TupleDesc
                                   , leftChild :: PhysicalPlan
                                   , rightChild :: PhysicalPlan
                                   }
                  | CreateTable { tableSchema :: TableSchema }
                  deriving (Eq, Show)

-- |Pick the best physical plan among all possible ones for a logical plan
getPhysicalPlan :: (StorageBackend m) => L.LogicalPlan -> Kyuu m PhysicalPlan
getPhysicalPlan lp = do
        ps <- genPhysicalPlans lp
        return $ head ps

-- |Generate all possible physical plans for a logical plan
genPhysicalPlans :: (StorageBackend m) => L.LogicalPlan -> Kyuu m [PhysicalPlan]

genPhysicalPlans (L.DataSource tableId tableName sArgs schema) =
        return [TableScan tableId tableName sArgs schema]

genPhysicalPlans (L.Selection conds schema child) = do
        childPlan <- getPhysicalPlan child
        return [Selection conds schema childPlan]

genPhysicalPlans (L.Projection exprs schema child) = do
        childPlan <- getPhysicalPlan child
        return [Projection exprs schema childPlan]

genPhysicalPlans lp@L.Join{} = do
        nestedLoopJoin <- genNestedLoopJoin lp
        return [nestedLoopJoin]

genPhysicalPlans (L.CreateTable schema) = do
        return [CreateTable schema]

genNestedLoopJoin :: (StorageBackend m) => L.LogicalPlan -> Kyuu m PhysicalPlan
genNestedLoopJoin (L.Join joinType joinQuals otherQuals schema left right) = do
        leftPlan  <- getPhysicalPlan left
        rightPlan <- getPhysicalPlan right
        return $ NestedLoopJoin joinType
                                joinQuals
                                otherQuals
                                schema
                                leftPlan
                                rightPlan