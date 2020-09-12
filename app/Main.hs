module Main where

import Control.Concurrent.Async
import qualified Data.ByteString as B
import Data.Char (ord)
import Data.Default.Class
import Kyuu
import Kyuu.Catalog.Catalog
import Kyuu.Catalog.Schema
import Kyuu.Config
import Kyuu.Core
import Kyuu.Error
import Kyuu.Executor.Builder
import Kyuu.Executor.Executor
import Kyuu.Executor.Operators
import Kyuu.Parse.Analyzer
import qualified Kyuu.Planner.LogicalPlan as L
import qualified Kyuu.Planner.PhysicalPlan as P
import Kyuu.Planner.RuleBasedOptimizer
import Kyuu.Prelude
import Kyuu.Storage.SuziQ.Backend
import Text.Pretty.Simple (pPrint)

execSimpleStmt :: (StorageBackend m) => String -> Kyuu m ()
execSimpleStmt stmt = case parseSQLStatement stmt of
  (Right tree) -> do
    void $ startTransaction
    liftIO $ putStrLn "=============================="
    liftIO $ pPrint tree
    liftIO $ putStrLn "=============================="
    qry <- analyzeParseTree tree
    liftIO $ pPrint qry
    liftIO $ putStrLn "=============================="

    physicalPlan <-
      if L.isOptimizableQuery qry
        then do
          (logicalPlan, pbState) <- L.buildLogicalPlan qry
          liftIO $ pPrint pbState
          liftIO $ pPrint logicalPlan
          liftIO $
            putStrLn
              "=============================="
          logicalPlan <-
            optimizeLogicalPlan
              pbState
              logicalPlan
          liftIO $ pPrint logicalPlan
          liftIO $
            putStrLn
              "=============================="
          P.getPhysicalPlan logicalPlan
        else P.buildPhysicalPlanForQuery qry

    liftIO $ pPrint physicalPlan
    liftIO $ putStrLn "=============================="
    execPlan <- buildExecPlan physicalPlan
    liftIO $ pPrint execPlan
    liftIO $ putStrLn "=============================="
    executePlan execPlan
    finishTransaction
  _ -> return ()

prog1 :: (StorageBackend m) => Kyuu m ()
prog1 = do
  execSimpleStmt
    "create table emp (empno int, ename varchar, sal double, deptno int)"
  execSimpleStmt "insert into emp (empno, ename) values (0, 'hello')"
  execSimpleStmt "select empno, ename from emp"

prog2 :: (StorageBackend m) => Kyuu m ()
prog2 = do
  execSimpleStmt
    "create table emp1 (empno int, ename varchar, sal double, deptno int)"
  execSimpleStmt "select * from pg_class"
  execSimpleStmt "select * from pg_attribute"

main :: IO ()
main = do
  sqInit
  db <- sqCreateDB "testdb"
  case db of
    (Just db) -> do
      threads <- runKyuu def [prog2]
      void $
        forConcurrently threads $ \thread ->
          runSuziQWithDB db thread
    _ -> putStrLn "cannot create database"
