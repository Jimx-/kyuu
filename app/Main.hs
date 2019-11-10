module Main where

import           Kyuu.SuziQ.Backend
import           Kyuu.Storage.Backend

import           Kyuu.Prelude
import           Kyuu.Core
import           Kyuu.Error

import qualified Data.ByteString               as B
import           Data.Char                      ( ord )

import           System.IO

packStr = B.pack . map (fromIntegral . ord)

insertTuples :: (StorageBackend m) => Int -> TableType m -> B.ByteString -> m ()
insertTuples i table tuple = if i == 0
        then return ()
        else do
                insertTuple table tuple
                insertTuples (i - 1) table tuple

scanTable :: (StorageBackend m) => Int -> TableScanIteratorType m -> m Int
scanTable count iterator = do
        tuple <- tableScanNext iterator Forward
        case tuple of
                (Just tuple) -> do
                        tData <- getTupleData tuple
                        liftIO $ print tData
                        scanTable (count + 1) iterator
                _ -> return count

prog :: (StorageBackend m) => Kyuu m ()
prog = do
        table <- createTable 0 0
        let tuple = packStr "hello"
        insertTuples 100 table tuple

        iterator <- beginTableScan table
        count    <- scanTable 0 iterator
        liftIO $ print count

main :: IO ()
main = runSuziQ "testdb" $ runKyuu prog
