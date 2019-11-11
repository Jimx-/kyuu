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

import           System.IO

packStr = B.pack . map (fromIntegral . ord)

insertTuples :: (StorageBackend m) => Int -> TableType m -> B.ByteString -> m ()
insertTuples i table tuple = if i == 0
        then return ()
        else do
                insertTuple table tuple
                insertTuples (i - 1) table tuple

prog :: (StorageBackend m) => Kyuu m ()
prog = do
        table <- createTable 0 1
        let tuple = packStr "0,hello,100,0"
        insertTuples 100 table tuple

        createTableWithCatalog
                (TableSchema
                        1
                        "emp"
                        [ ColumnSchema 1 1 "empno"  SInt
                        , ColumnSchema 1 2 "ename"  SString
                        , ColumnSchema 1 3 "sal"    SDouble
                        , ColumnSchema 1 4 "deptno" SInt
                        ]
                )

        let sqlStmt = "select ename from emp"
        case parseSQLStatement sqlStmt of
                (Right tree) -> do
                        liftIO $ putStrLn "=============================="
                        liftIO $ pPrint tree
                        liftIO $ putStrLn "=============================="
                        qry <- analyzeParseTree tree
                        liftIO $ pPrint qry
                        liftIO $ putStrLn "=============================="
                        (logicalPlan, pbState) <- L.buildLogicalPlan qry
                        liftIO $ pPrint pbState
                        liftIO $ pPrint logicalPlan
                        liftIO $ putStrLn "=============================="
                        logicalPlan <- optimizeLogicalPlan pbState logicalPlan
                        liftIO $ pPrint logicalPlan
                        liftIO $ putStrLn "=============================="
                        physicalPlan <- P.getPhysicalPlan logicalPlan
                        liftIO $ pPrint physicalPlan
                        liftIO $ putStrLn "=============================="
                        execPlan <- buildExecPlan physicalPlan
                        liftIO $ pPrint execPlan
                        liftIO $ putStrLn "=============================="
                        executePlan execPlan
                _ -> return ()

main :: IO ()
main = runSuziQ "testdb" $ runKyuu prog
