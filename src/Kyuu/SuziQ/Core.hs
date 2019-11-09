{-# LANGUAGE ConstraintKinds, FlexibleContexts, TemplateHaskell #-}
{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, TypeFamilies #-}
module Kyuu.SuziQ.Core
        ( SuziQ
        , runSuziQ
        , getDB
        , lerror
        )
where

import           Control.Lens
import           Control.Monad.Trans.State.Lazy
import           Control.Monad.Trans.Except
import qualified Control.Monad.Trans.Identity  as I
import           Control.Monad.Trans.Class

import           Kyuu.Prelude            hiding ( get )
import           Kyuu.SuziQ.DB                 as S
import           Kyuu.SuziQ.Error               ( SqErr(..) )

import           Kyuu.Storage.Backend

newtype SqState = SqState { _db :: SqDB }
            deriving (Eq, Show)

makeLensesWith (lensRules & lensField .~ lensGen) ''SqState

type SuziQ = I.IdentityT (StateT SqState (ExceptT SqErr IO))

getDB :: SuziQ SqDB
getDB = lift $ (^. db_) <$> get

lcatch :: SuziQ a -> (SqErr -> SuziQ a) -> SuziQ a
lcatch = I.liftCatch (liftCatch catchE)

lerror :: SqErr -> SuziQ a
lerror = lift . lift . throwE

runSuziQ :: String -> SuziQ () -> IO ()
runSuziQ rootPath prog = do
        db <- createDB rootPath
        case db of
                (Just db) -> do
                        let initState = SqState { _db = db }
                        res <- runExceptT
                                $ runStateT (I.runIdentityT prog) initState
                        case res of
                                Left err ->
                                        putStrLn
                                                $  "Uncaught error: "
                                                ++ (show err)
                                Right _ -> return ()
                Nothing -> putStrLn "cannot open database"

instance StorageBackend SuziQ where
        type TableType SuziQ = S.SqTable
        createTable dbId tableId = do
                db    <- getDB
                table <- S.createTable db dbId tableId
                case table of
                        (Just table) -> return table
                        _            -> lerror (Msg "cannot create table")

        insertTuple table tuple = do
                db <- getDB
                S.tableInsertTuple table db tuple
