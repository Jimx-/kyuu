module Kyuu.Error
        ( Err(..)
        )
where

data Err = Msg String
         | TableNotFound String
         | TableExists String
         | ColumnNotFound String
         | DuplicateColumn String
         | UnknownDataType String
         deriving (Eq, Ord)

instance Show Err where
        show (Msg             s   ) = s
        show (TableNotFound   name) = "Table '" ++ name ++ "' not found"
        show (TableExists     name) = "Table '" ++ name ++ "' exists"
        show (ColumnNotFound  name) = "Column '" ++ name ++ "' not found"
        show (DuplicateColumn name) = "Duplicate column '" ++ name ++ "'"
        show (UnknownDataType name) = "Unknown data type: " ++ name
