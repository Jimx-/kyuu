{-# LANGUAGE ForeignFunctionInterface #-}
module Kyuu.SuziQ.FFI
        ( SqDB
        , SqTable
        , SqTableScanIterator
        , SqTuple
        , sq_last_error_length
        , sq_last_error_message
        , sq_create_db
        , sq_free_db
        , sq_create_table
        , sq_free_table
        , sq_table_insert_tuple
        , sq_table_begin_scan
        , sq_free_table_scan_iterator
        , sq_table_scan_next
        , sq_free_tuple
        , sq_tuple_get_data
        , sq_tuple_get_data_len
        )
where

import           Kyuu.Prelude

import           Data.Int                       ( Int64 )
import qualified Data.ByteString               as B
import           Foreign.Ptr
import           Foreign.ForeignPtr
import           Foreign.C.Types                ( CChar
                                                , CInt(..)
                                                )
import           Foreign.C.String               ( CString(..)
                                                , newCString
                                                , withCString
                                                )

data DBPtr
data TablePtr
data TableScanIteratorPtr
data TuplePtr

type SqDB = ForeignPtr DBPtr
type SqTable = ForeignPtr TablePtr
type SqTableScanIterator = ForeignPtr TableScanIteratorPtr
type SqTuple = ForeignPtr TuplePtr

foreign import ccall unsafe "sq_last_error_length"
  sq_last_error_length :: IO CInt

foreign import ccall unsafe "sq_last_error_message"
  sq_last_error_message :: CString -> CInt -> IO CInt

foreign import ccall unsafe "sq_create_db"
  sq_create_db :: CString -> IO (Ptr DBPtr)

foreign import ccall unsafe "&sq_free_db"
  sq_free_db :: FunPtr (Ptr DBPtr -> IO ())

foreign import ccall unsafe "sq_create_table"
  sq_create_table :: Ptr DBPtr -> Int64 -> Int64 -> IO (Ptr TablePtr)

foreign import ccall unsafe "&sq_free_table"
  sq_free_table :: FunPtr (Ptr TablePtr -> IO ())

foreign import ccall unsafe "sq_table_insert_tuple"
  sq_table_insert_tuple :: Ptr TablePtr -> Ptr DBPtr -> Ptr CChar -> CInt -> IO CInt

foreign import ccall unsafe "sq_table_begin_scan"
  sq_table_begin_scan :: Ptr TablePtr -> Ptr DBPtr -> IO (Ptr TableScanIteratorPtr)

foreign import ccall unsafe "&sq_free_table_scan_iterator"
  sq_free_table_scan_iterator :: FunPtr (Ptr TableScanIteratorPtr -> IO ())

foreign import ccall unsafe "sq_table_scan_next"
  sq_table_scan_next :: Ptr TableScanIteratorPtr -> Ptr DBPtr -> CInt -> IO (Ptr TuplePtr)

foreign import ccall unsafe "&sq_free_tuple"
  sq_free_tuple :: FunPtr (Ptr TuplePtr -> IO ())

foreign import ccall unsafe "sq_tuple_get_data_len"
  sq_tuple_get_data_len :: Ptr TuplePtr -> IO CInt

foreign import ccall unsafe "sq_tuple_get_data"
  sq_tuple_get_data :: Ptr TuplePtr -> Ptr CChar -> CInt -> IO CInt
