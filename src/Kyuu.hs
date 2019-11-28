module Kyuu
        ( runKyuu
        )
where

import           Kyuu.Prelude
import           Kyuu.Core
import           Kyuu.Catalog.State
import           Kyuu.Catalog.Catalog
import           Kyuu.Storage.Backend

import           Control.Applicative
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TVar
import           Control.Concurrent.STM.TMVar

import           Control.Monad.Trans.Except
import           Control.Monad.Except
import           Control.Monad.Trans.Class

-- |Perform all effects produced by the query processor
runKyuu :: (StorageBackend m, MonadIO t) => [Kyuu m ()] -> t [m ()]
runKyuu workers = do
        let cs = initCatalogState
        mcs   <- liftIO $ newTVarIO cs
        cq    <- liftIO newTQueueIO
        ready <- liftIO $ atomically newEmptyTMVar

        let     progs           = checkpointerThread cq : workers
                bootstrapThread = void $ runExceptT $ runStateT
                        (bootstrapProg (length progs) ready)
                        (initKyuuState mcs cq)

        threads <- forM progs $ \prog -> return $ do
                liftIO $ atomically $ takeTMVar ready
                res <- runExceptT $ runStateT prog (initKyuuState mcs cq)
                case res of
                        Left err ->
                                liftIO
                                        $  putStrLn
                                        $  "Uncaught error: "
                                        ++ show err
                        Right _ -> return ()

        return $ bootstrapThread : threads

    where
        bootstrapProg n ready = do
                bootstrapCatalog
                forM_ [1 .. n] $ \_ -> liftIO $ atomically $ putTMVar ready 1

        checkpointerThread cq = do
                delay <- liftIO $ registerDelay 300000000
                liftIO
                        $   atomically
                        $   Just
                        <$> readTQueue cq
                        <|> Nothing
                        <$  (check <=< readTVar) delay

                createCheckpoint

                checkpointerThread cq
