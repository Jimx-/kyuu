module Kyuu.Error
  ( Err (..),
  )
where

import Kyuu.Prelude

data Err
  = Msg String
  | TableNotFound OID
  | TableWithNameNotFound String
  | TableExists String
  | ColumnNotFound String
  | ColumnNumNotFound OID
  | DuplicateColumn String
  | IndexNotFound OID
  | UnknownDataType String
  | InvalidState String
  | SyntaxError String
  | DataCorrupted String
  | InvalidExpression String
  deriving (Eq, Ord)

instance Show Err where
  show (Msg s) = s
  show (TableNotFound id) = "Table " ++ show id ++ " not found"
  show (TableWithNameNotFound name) =
    "Table with name '" ++ name ++ "' not found"
  show (TableExists name) = "Table with name '" ++ name ++ "' exists"
  show (ColumnNotFound name) = "Column '" ++ name ++ "' not found"
  show (ColumnNumNotFound num) = "Column " ++ show num ++ " not found"
  show (DuplicateColumn name) = "Duplicate column '" ++ name ++ "'"
  show (IndexNotFound id) = "Index " ++ show id ++ " not found"
  show (UnknownDataType name) = "Unknown data type: " ++ name
  show (InvalidState s) = "Invalid state: " ++ s
  show (SyntaxError s) = "Syntax error: " ++ s
  show (DataCorrupted s) = "Data corrupted: " ++ s
  show (InvalidExpression s) = "Invalid expression: " ++ s
