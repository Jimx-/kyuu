module Kyuu.Storage.SuziQ.Error
  ( SqErr (..),
  )
where

newtype SqErr = Msg String
  deriving (Eq, Ord)

instance Show SqErr where
  show (Msg s) = s
