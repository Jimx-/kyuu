module Kyuu.Value
        ( Value(..)
        , ColumnDesc(..)
        , TupleDesc
        , Tuple(..)
        , sortTuple
        , decodeTuple
        )
where

import           Kyuu.Prelude
import           Kyuu.Catalog.Schema

import qualified Data.ByteString               as B
import           Data.Char                      ( chr )
import           Data.List                      ( elemIndex )
import           Data.List.Split
import           Data.Monoid

data Value = VNull
           | VBool Bool
           | VInt Int
           | VDouble Double
           | VString String
           deriving (Eq)

instance Show Value where
        show VNull       = "(null)"
        show (VBool   b) = show b
        show (VInt    i) = show i
        show (VDouble d) = show d
        show (VString s) = "\"" ++ s ++ "\""

data ColumnDesc = ColumnDesc OID OID
                deriving (Eq, Show)

type TupleDesc = [ColumnDesc]

data Tuple = Tuple TupleDesc [Value]
           deriving (Eq, Show)

instance Semigroup Tuple where
        (Tuple ld lv) <> (Tuple rd rv) = Tuple (ld <> rd) (lv <> rv)

instance Monoid Tuple where
        mempty = Tuple [] []

sortTuple :: TupleDesc -> Tuple -> Tuple
sortTuple [] _ = mempty
sortTuple (d : ds) tuple@(Tuple descs values) =
        (case elemIndex d descs of
                        Just index -> Tuple [descs !! index] [values !! index]
                        Nothing    -> mempty
                )
                <> sortTuple ds tuple

decodeTuple :: B.ByteString -> TableSchema -> Tuple
decodeTuple buf TableSchema { tableCols = tableCols } = readValues
        (splitOn "," $ map (chr . fromIntegral) (B.unpack buf))
        tableCols
    where
        readValues [] _ = mempty
        readValues (x : xs) (ColumnSchema tableId colId _ sType : cols) =
                let val     = readValue sType x
                    colDesc = ColumnDesc tableId colId
                in  Tuple [colDesc] [val] <> readValues xs cols

        readValue SInt    x = VInt (read x)
        readValue SDouble x = VDouble (read x)
        readValue SString x = VString x
