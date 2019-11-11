module Kyuu.Error
        ( Err(..)
        )
where

import           Kyuu.Prelude

data Err = Msg String
         | TableNotFound OID
         | TableWithNameNotFound String
         | TableExists String
         | ColumnNotFound String
         | DuplicateColumn String
         | UnknownDataType String
         | InvalidState String
         deriving (Eq, Ord)

instance Show Err where
        show (Msg           s ) = s
        show (TableNotFound id) = "Table " ++ show id ++ " not found"
        show (TableWithNameNotFound name) =
                "Table with name '" ++ name ++ "' not found"
        show (TableExists     name) = "Table with name '" ++ name ++ "' exists"
        show (ColumnNotFound  name) = "Column '" ++ name ++ "' not found"
        show (DuplicateColumn name) = "Duplicate column '" ++ name ++ "'"
        show (UnknownDataType name) = "Unknown data type: " ++ name
        show (InvalidState    s   ) = "Invalid state: " ++ s
