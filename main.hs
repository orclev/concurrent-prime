module Main where

import ClassyPrelude.Conduit as C
import Data.List (last)
import qualified Data.List as L
import Data.Conduit
import Data.Conduit.TMChan
import Data.Conduit.List as CL
import Data.Array.MArray (getElems, newListArray, writeArray)
import Control.Concurrent (forkIO, ThreadId)
import Control.Concurrent.STM
import Control.Concurrent.STM.TSkipList as SL
import Data.Map (elems)

type IntType = Integer

upperBound :: Integral a => a -> a
upperBound = floor . sqrt . fromRational . toRational

potentialPrimesList :: IntType -> [IntType]
potentialPrimesList target = C.filter odd [3..upperBound target]

potentialPrimesArray :: IntType -> STM (TSkipList IntType IntType)
potentialPrimesArray target = do
    x <- new 0.5 5
    sequence_ [ SL.insert i i x | i <- potentialPrimesList target]
    return x

main :: IO ()
main = do
    (target, numberOfThreads) <- readArgs
    -- let
    --    (target, numberOfThreads) =
    --        case args of
    --            [] -> 8000
    --            [x] -> (read x, 4)
    --            [x, y] -> (read x, read y)
    --            _ -> error "usage: ./concurrent-prime [target] [number of workers] +RTS -N"
    output <- startWorkers target numberOfThreads
    sourceTBMChan output $$ CL.mapM_ (liftIO . putStrLn . show)

startWorkers :: IntType -> Int -> IO (TBMChan IntType)
startWorkers target numberOfThreads = do
    potentialPrimes <- atomically $ potentialPrimesArray target
    inputs <- atomically $ newTBMChan 100
    output <- atomically $ newTBMChan 100
    C.mapM_ (\_ -> spawnWorker potentialPrimes inputs output) [1..numberOfThreads]
    forkIO . runResourceT $ (source target) $$ sinkTBMChan inputs
    return output
    where
        source target = unfold (\a -> if a > target then Nothing else Just (a,a+2)) 3

spawnWorker :: TSkipList IntType IntType -> TBMChan IntType -> TBMChan IntType -> IO ThreadId
spawnWorker checks inputs output = forkIO $ sourceTBMChan inputs
    $$ CL.mapMaybeM isPrime =$ sinkTBMChan output
    where
        isPrime :: IntType -> IO (Maybe IntType)
        isPrime x = do
            possiblePrimes <- atomically $ leq (upperBound x) checks
            if all (check x) (elems possiblePrimes) then return (Just x) else
                atomically (SL.delete x checks) >> return Nothing
        check :: IntType -> IntType -> Bool
        check x y = x `rem` y /= 0
