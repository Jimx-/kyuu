module Main where

import           Kyuu.SuziQ.DB

import qualified Data.ByteString               as B
import           Data.Char                      ( ord )

packStr = B.pack . map (fromIntegral . ord)

insertTuples :: Int -> Table -> DB -> B.ByteString -> IO ()
insertTuples i table db tuple = if i == 0
        then return ()
        else do
                tableInsertTuple table db tuple
                insertTuples (i - 1) table db tuple

main :: IO ()
main = do
        db <- createDB "testdb"
        case db of
                (Just db) -> do
                        table <- createTable db 0 0
                        case table of
                                (Just table) -> do
                                        let tuple = packStr "hello"
                                        insertTuples 100 table db tuple
                                Nothing -> return ()
                Nothing -> return ()
