{-# LANGUAGE ForeignFunctionInterface #-}

module Kyuu.Storage.SuziQ.FFI
  ( SqDB,
    SqTable,
    SqTableScanIterator,
    SqTuple,
    SqTupleSlot,
    SqIndex (..),
    SqIndexScanIterator (..),
    SqTransaction,
    sq_init,
    sq_last_error_length,
    sq_last_error_message,
    sq_create_db,
    sq_free_db,
    sq_create_table,
    sq_open_table,
    sq_free_table,
    sq_create_index,
    sq_open_index,
    sq_free_index,
    sq_start_transaction,
    sq_free_transaction,
    sq_commit_transaction,
    sq_table_insert_tuple,
    sq_free_item_pointer,
    sq_table_begin_scan,
    sq_free_table_scan_iterator,
    sq_table_scan_next,
    sq_free_tuple,
    sq_tuple_get_data,
    sq_tuple_get_data_len,
    sq_create_checkpoint,
    sq_get_next_oid,
    sq_index_insert,
    RawIndexKeyComparatorFunc,
    sqWrapRawIndexKeyComparator,
    sq_index_begin_scan,
    sq_free_index_scan_iterator,
    RawIndexScanPredicate,
    sqWrapRawIndexScanPredicate,
    sq_index_rescan,
    sq_index_scan_next,
  )
where

import qualified Data.ByteString as B
import Data.Int (Int64)
import Foreign.C.String
  ( CString (..),
    newCString,
    withCString,
  )
import Foreign.C.Types
  ( CChar,
    CInt (..),
  )
import Foreign.ForeignPtr
import Foreign.Ptr
import Kyuu.Prelude

data DBPtr

data TablePtr

data TableScanIteratorPtr

data TuplePtr

data TupleSlotPtr

data IndexPtr

data IndexScanIteratorPtr

data TransactionPtr

type SqDB = ForeignPtr DBPtr

type SqTable = ForeignPtr TablePtr

type SqTableScanIterator = ForeignPtr TableScanIteratorPtr

type SqTuple = ForeignPtr TuplePtr

type SqTupleSlot = ForeignPtr TupleSlotPtr

data SqIndex = SqIndex
  { keyComp :: Maybe (FunPtr RawIndexKeyComparatorFunc),
    index :: ForeignPtr IndexPtr
  }

data SqIndexScanIterator = SqIndexScanIterator
  { predicate :: Maybe (FunPtr RawIndexScanPredicate),
    iterator :: ForeignPtr IndexScanIteratorPtr
  }

type SqTransaction = ForeignPtr TransactionPtr

foreign import ccall unsafe "sq_init"
  sq_init :: IO ()

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

foreign import ccall unsafe "sq_open_table"
  sq_open_table :: Ptr DBPtr -> Int64 -> Int64 -> IO (Ptr TablePtr)

foreign import ccall unsafe "&sq_free_table"
  sq_free_table :: FunPtr (Ptr TablePtr -> IO ())

foreign import ccall unsafe "sq_create_index"
  sq_create_index :: Ptr DBPtr -> Int64 -> Int64 -> FunPtr RawIndexKeyComparatorFunc -> IO (Ptr IndexPtr)

foreign import ccall unsafe "sq_open_index"
  sq_open_index :: Ptr DBPtr -> Int64 -> Int64 -> FunPtr RawIndexKeyComparatorFunc -> IO (Ptr IndexPtr)

foreign import ccall unsafe "&sq_free_index"
  sq_free_index :: FunPtr (Ptr IndexPtr -> IO ())

foreign import ccall unsafe "sq_start_transaction"
  sq_start_transaction :: Ptr DBPtr -> CInt -> IO (Ptr TransactionPtr)

foreign import ccall unsafe "&sq_free_transaction"
  sq_free_transaction :: FunPtr (Ptr TransactionPtr -> IO ())

foreign import ccall unsafe "sq_commit_transaction"
  sq_commit_transaction :: Ptr DBPtr -> Ptr TransactionPtr -> IO ()

foreign import ccall unsafe "sq_table_insert_tuple"
  sq_table_insert_tuple :: Ptr TablePtr -> Ptr DBPtr -> Ptr TransactionPtr -> Ptr CChar -> CInt -> IO (Ptr TupleSlotPtr)

foreign import ccall unsafe "&sq_free_item_pointer"
  sq_free_item_pointer :: FunPtr (Ptr TupleSlotPtr -> IO ())

foreign import ccall unsafe "sq_table_begin_scan"
  sq_table_begin_scan :: Ptr TablePtr -> Ptr DBPtr -> Ptr TransactionPtr -> IO (Ptr TableScanIteratorPtr)

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

foreign import ccall unsafe "sq_create_checkpoint"
  sq_create_checkpoint :: Ptr DBPtr -> IO ()

foreign import ccall unsafe "sq_get_next_oid"
  sq_get_next_oid :: Ptr DBPtr -> IO Int64

type RawIndexKeyComparatorFunc =
  Ptr CChar -> CInt -> Ptr CChar -> CInt -> IO CInt

foreign import ccall "sq_index_insert"
  sq_index_insert :: Ptr IndexPtr -> Ptr DBPtr -> Ptr CChar -> CInt -> Ptr TupleSlotPtr -> IO ()

foreign import ccall "wrapper"
  sqWrapRawIndexKeyComparator :: RawIndexKeyComparatorFunc -> IO (FunPtr RawIndexKeyComparatorFunc)

foreign import ccall "sq_index_begin_scan"
  sq_index_begin_scan :: Ptr IndexPtr -> Ptr DBPtr -> Ptr TransactionPtr -> Ptr TablePtr -> IO (Ptr IndexScanIteratorPtr)

foreign import ccall unsafe "&sq_free_index_scan_iterator"
  sq_free_index_scan_iterator :: FunPtr (Ptr IndexScanIteratorPtr -> IO ())

type RawIndexScanPredicate = Ptr CChar -> CInt -> IO CInt

foreign import ccall "wrapper"
  sqWrapRawIndexScanPredicate :: RawIndexScanPredicate -> IO (FunPtr RawIndexScanPredicate)

foreign import ccall "sq_index_rescan"
  sq_index_rescan :: Ptr IndexScanIteratorPtr -> Ptr DBPtr -> Ptr CChar -> CInt -> FunPtr RawIndexScanPredicate -> IO ()

foreign import ccall "sq_index_scan_next"
  sq_index_scan_next :: Ptr IndexScanIteratorPtr -> Ptr DBPtr -> CInt -> IO (Ptr TuplePtr)
