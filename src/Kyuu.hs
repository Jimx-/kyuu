module Kyuu
  ( runKyuu,
  )
where

import Control.Applicative
import Control.Concurrent.STM
import Control.Concurrent.STM.TMVar
import Control.Concurrent.STM.TVar
import Control.Monad.Except
import Control.Monad.Trans.Class
import Control.Monad.Trans.Except
import Kyuu.Catalog.Catalog
import Kyuu.Catalog.State
import Kyuu.Config
import Kyuu.Core
import Kyuu.Prelude
import Kyuu.Storage.Backend

-- | Perform all effects produced by the query processor
runKyuu :: (StorageBackend m, MonadIO t) => Config -> [Kyuu m ()] -> t [m ()]
runKyuu config workers = do
  let cs = initCatalogState
  mcs <- liftIO $ newTMVarIO cs
  cq <- liftIO newTQueueIO
  ready <- liftIO newEmptyTMVarIO

  let progs = checkpointerThread cq : workers
      bootstrapThread =
        void $
          runExceptT $
            flip runStateT (initKyuuState mcs cq) $
              runReaderT
                (bootstrapProg (length progs) ready)
                config

  threads <- forM progs $ \prog -> return $ do
    liftIO $ atomically $ takeTMVar ready
    res <-
      runExceptT $
        flip runStateT (initKyuuState mcs cq) $
          runReaderT prog config
    case res of
      Left err ->
        liftIO $
          putStrLn $
            "Uncaught error: "
              ++ show err
      Right _ -> return ()

  return $ bootstrapThread : threads
  where
    bootstrapProg n ready = do
      bootstrapCatalog
      forM_ [1 .. n] $ \_ -> liftIO $ atomically $ putTMVar ready 1

    checkpointerThread cq = do
      delay <- liftIO $ registerDelay 300000000
      liftIO $
        atomically $
          Just
            <$> readTQueue cq
            <|> Nothing
            <$ (check <=< readTVar) delay

      createCheckpoint

      checkpointerThread cq
