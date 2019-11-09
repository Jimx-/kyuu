module Kyuu.SuziQ.Error
        ( SqErr(..)
        )
where

data SqErr = Msg String
         deriving (Eq, Ord)

instance Show SqErr where
        show (Msg s) = s
