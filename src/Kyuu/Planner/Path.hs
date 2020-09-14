module Kyuu.Planner.Path
  ( Path (..),
    mkScanPath,
    mkIndexPath,
  )
where

import Kyuu.Catalog.Schema
import Kyuu.Expression
import Kyuu.Prelude
import Kyuu.Value

data Path
  = ScanPath
      { tableId :: OID,
        tableName :: String,
        searchArgs :: [SqlExpr Value]
      }
  | IndexPath
      { tableId :: OID,
        indexSchema :: IndexSchema,
        indexClauses :: [SqlExpr Value],
        searchArgs :: [SqlExpr Value]
      }
  deriving (Show)

mkScanPath :: OID -> String -> [SqlExpr Value] -> Path
mkScanPath = ScanPath

mkIndexPath :: OID -> IndexSchema -> [SqlExpr Value] -> [SqlExpr Value] -> Path
mkIndexPath = IndexPath
