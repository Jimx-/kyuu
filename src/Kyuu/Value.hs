{-# LANGUAGE DeriveGeneric, NamedFieldPuns #-}
module Kyuu.Value
        ( Value(..)
        , ColumnDesc(..)
        , TupleDesc
        , Tuple(..)
        , sortTuple
        , encodeTuple
        , decodeTuple
        , decodeTupleValues
        )
where

import           Kyuu.Prelude
import           Kyuu.Catalog.Schema

import qualified Data.ByteString               as B
import           Data.Char                      ( chr )
import           Data.List                      ( elemIndex )
import           Data.Monoid
import           Data.Store
import           Data.Typeable
import           GHC.Generics

data Value = VNull
           | VBool Bool
           | VInt Int
           | VDouble Double
           | VString String
           deriving (Eq, Generic, Typeable)
instance Store Value

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

encodeTuple :: Tuple -> B.ByteString
encodeTuple (Tuple _ vals) = encode vals

decodeTuple :: B.ByteString -> TableSchema -> Maybe Tuple
decodeTuple buf TableSchema { tableCols } = case decodeTupleValues buf of
        (Just (Tuple [] vs)) -> Just $ Tuple tupleDesc vs
        _                    -> Nothing
    where
        tupleDesc = map
                (\ColumnSchema { colTable, colId } -> ColumnDesc colTable colId)
                tableCols

decodeTupleValues :: B.ByteString -> Maybe Tuple
decodeTupleValues buf = case decode buf of
        (Right vals) -> Just $ Tuple [] vals
        _            -> Nothing
