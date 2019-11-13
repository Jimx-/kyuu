module Main where

import           Kyuu.Prelude
import           Kyuu.Core
import           Kyuu.Error

import           Kyuu.Catalog.Schema
import           Kyuu.Catalog.Catalog

import           Kyuu.Parse.Analyzer

import qualified Kyuu.Planner.LogicalPlan      as L
import qualified Kyuu.Planner.PhysicalPlan     as P
import           Kyuu.Planner.RuleBasedOptimizer

import           Kyuu.Executor.Operators
import           Kyuu.Executor.Builder
import           Kyuu.Executor.Executor

import           Kyuu.Storage.SuziQ.Backend
import           Kyuu.Storage.Backend

import qualified Data.ByteString               as B
import           Data.Char                      ( ord )

import           Text.Pretty.Simple             ( pPrint )

execSimpleStmt :: (StorageBackend m) => String -> Kyuu m ()
execSimpleStmt stmt = case parseSQLStatement stmt of
        (Right tree) -> do
                liftIO $ putStrLn "=============================="
                liftIO $ pPrint tree
                liftIO $ putStrLn "=============================="
                qry <- analyzeParseTree tree
                liftIO $ pPrint qry
                liftIO $ putStrLn "=============================="

                physicalPlan <- if L.isOptimizableQuery qry
                        then do
                                (logicalPlan, pbState) <- L.buildLogicalPlan qry
                                liftIO $ pPrint pbState
                                liftIO $ pPrint logicalPlan
                                liftIO $ putStrLn
                                        "=============================="
                                logicalPlan <- optimizeLogicalPlan
                                        pbState
                                        logicalPlan
                                liftIO $ pPrint logicalPlan
                                liftIO $ putStrLn
                                        "=============================="
                                P.getPhysicalPlan logicalPlan
                        else P.buildPhysicalPlanForQuery qry

                liftIO $ pPrint physicalPlan
                liftIO $ putStrLn "=============================="
                execPlan <- buildExecPlan physicalPlan
                liftIO $ pPrint execPlan
                liftIO $ putStrLn "=============================="
                executePlan execPlan

        _ -> return ()

prog :: (StorageBackend m) => Kyuu m ()
prog = do
        execSimpleStmt
                "create table emp (empno int, ename varchar, sal double, deptno int)"
        execSimpleStmt "insert into emp (empno, ename) values (0, 'hello')"
        execSimpleStmt "select empno, ename from emp"

main :: IO ()
main = runSuziQ "testdb" $ runKyuu prog
