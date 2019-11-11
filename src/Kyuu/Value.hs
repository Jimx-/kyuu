module Kyuu.Value
        ( Value(..)
        , ColumnDesc(..)
        , TupleDesc
        , Tuple(..)
        , sortTuple
        )
where

import           Data.List                      ( elemIndex )
import           Data.Monoid

import           Kyuu.Prelude

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
