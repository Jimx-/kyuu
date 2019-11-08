module Main where

import           Kyuu.SuziQ.DB

main :: IO ()
main = do
        db <- createDB "testdb"
        case db of
                (Just db) -> do
                        table <- createTable db 0 0
                        return ()
                Nothing -> return ()
