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
import Kyuu.Planner.PhysicalPlanOptimizer
import Kyuu.Planner.RuleBasedOptimizer
import Kyuu.Prelude
import Kyuu.Storage.SuziQ.Backend
import System.IO
import Text.Pretty.Simple (pPrint)

execSimpleStmt :: (StorageBackend m) => String -> Kyuu m ()
execSimpleStmt stmt = case parseSQLStatement stmt of
  (Right tree) -> do
    void startTransaction
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
          getPhysicalPlan logicalPlan
        else P.buildPhysicalPlanForQuery qry

    liftIO $ pPrint physicalPlan
    liftIO $ putStrLn "=============================="
    execPlan <- buildExecPlan physicalPlan
    liftIO $ pPrint execPlan
    liftIO $ putStrLn "=============================="
    executePlan execPlan
    finishTransaction
  _ -> lerror (SyntaxError "Cannot parse query statement")

prog1 :: (StorageBackend m) => Kyuu m ()
prog1 = do
  -- execSimpleStmt
  --   "create table emp (empno int, ename varchar, job varchar, sal double, deptno int)"
  -- execSimpleStmt
  --   "create table dept (deptno int, dname varchar, loc varchar)"

  -- execSimpleStmt "insert into dept (deptno, dname, loc) values (10, 'Accouting', 'New York')"
  -- execSimpleStmt "insert into dept (deptno, dname, loc) values (20, 'Research', 'Dallas')"
  -- execSimpleStmt "insert into dept (deptno, dname, loc) values (30, 'Sales', 'Chicago')"
  -- execSimpleStmt "insert into dept (deptno, dname, loc) values (40, 'Operations', 'Boston')"

  -- execSimpleStmt "insert into emp values (7839, 'King', 'President', 5000, 10)"
  -- execSimpleStmt "insert into emp values (7698, 'Blake', 'Manager', 2850, 30)"
  -- execSimpleStmt "insert into emp values (7782, 'Clark', 'Manager', 2459, 10)"
  -- execSimpleStmt "insert into emp values (7566, 'Jones', 'Manager', 2975, 20)"
  -- execSimpleStmt "insert into emp values (7788, 'Scott', 'Analyst', 3000, 20)"
  -- execSimpleStmt "insert into emp values (7902, 'Ford', 'Analyst', 3000, 20)"
  -- execSimpleStmt "insert into emp values (7369, 'Smith', 'Clerk', 800, 20)"
  -- execSimpleStmt "insert into emp values (7499, 'Allen', 'Salesman', 1600, 30)"
  -- execSimpleStmt "insert into emp values (7521, 'Ward', 'Salesman', 1250, 30)"
  -- execSimpleStmt "insert into emp values (7654, 'Martin', 'Salesman', 1250, 30)"
  -- execSimpleStmt "insert into emp values (7844, 'Turner', 'Salesman', 1500, 30)"
  -- execSimpleStmt "insert into emp values (7876, 'Adams', 'Clerk', 1100, 20)"
  -- execSimpleStmt "insert into emp values (7900, 'James', 'Clerk', 950, 30)"
  -- execSimpleStmt "insert into emp values (7934, 'Miller', 'Clerk', 1300, 10)"

  execSimpleStmt "select dname, count(1) from dept, emp where emp.deptno = dept.deptno and sal > 2000 group by dname"

-- execSimpleStmt "insert into emp (empno, ename, deptno) values (0, 'hello', 1)"
-- execSimpleStmt "insert into emp (empno, ename, deptno) values (1, 'world', 2)"
-- execSimpleStmt "insert into dept (id, name) values (1, 'dept_1')"
-- execSimpleStmt "insert into dept (id, name) values (2, 'dept_2')"
-- execSimpleStmt "select empno, ename, deptno from emp"
-- execSimpleStmt "select id, name from dept"

-- execSimpleStmt "select * from dept, emp where deptno = id"
-- execSimpleStmt
--   "create table agg (grp int, val int)"
-- execSimpleStmt "insert into agg (grp, val) values (0, 10)"
-- execSimpleStmt "insert into agg (grp, val) values (0, 20)"
-- execSimpleStmt "insert into agg (grp, val) values (1, 100)"
-- execSimpleStmt "insert into agg (grp, val) values (1, 200)"
-- execSimpleStmt "select * from pg_class where oid > 1"

-- execSimpleStmt "select grp, max(val) + min(val), count(1) from agg group by grp limit 100 offset 1"

prog2 :: (StorageBackend m) => Kyuu m ()
prog2 = do
  execSimpleStmt "select * from pg_class where oid > 1"

-- execSimpleStmt "select * from pg_attribute, pg_class"

cli :: (StorageBackend m) => Kyuu m ()
cli = do
  liftIO $ putStr "Q> "
  liftIO $ hFlush stdout
  stmt <- liftIO getLine
  execSimpleStmt stmt

  cli

main :: IO ()
main = do
  sqInit
  db <- sqCreateDB "127.0.0.1:9605"
  case db of
    (Just db) -> do
      threads <- runKyuu def [cli]
      void $
        forConcurrently threads $ \thread ->
          runSuziQWithDB db thread
    _ -> putStrLn "cannot create database"
