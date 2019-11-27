module Kyuu
        ( runKyuu
        )
where

import           Kyuu.Prelude
import           Kyuu.Core
import           Kyuu.Catalog.State
import           Kyuu.Catalog.Catalog
import           Kyuu.Storage.Backend

import           Control.Concurrent.MVar

import           Control.Monad.Trans.Except
import           Control.Monad.Except
import           Control.Monad.Trans.Class

-- |Perform all effects produced by the query processor
runKyuu :: (StorageBackend m, MonadIO t) => [Kyuu m ()] -> t [m ()]
runKyuu workers = do
        let cs = initCatalogState
        mcs   <- liftIO $ newMVar cs
        ready <- liftIO newEmptyMVar

        let bootstrapThread = void $ runExceptT $ runStateT
                    (bootstrapProg (length workers) ready)
                    (initKyuuState mcs)

        threads <- forM workers $ \worker -> return $ do
                _   <- liftIO $ takeMVar ready
                res <- runExceptT $ runStateT worker (initKyuuState mcs)
                case res of
                        Left err ->
                                liftIO
                                        $  putStrLn
                                        $  "Uncaught error: "
                                        ++ show err
                        Right _ -> return ()

        return $ bootstrapThread : threads

    where
        bootstrapProg :: (StorageBackend m) => Int -> MVar Int -> Kyuu m ()
        bootstrapProg n ready = do
                bootstrapCatalog
                forM_ [1 .. n] $ \_ -> liftIO $ putMVar ready 1
