module Main where

import ClassyPrelude.Conduit as C
import Data.Conduit
import Data.Conduit.TMChan
import Data.Conduit.List as CL
import Control.Concurrent (forkIO, ThreadId)
import Control.Concurrent.STM
import Control.Concurrent.STM.TSkipList as SL
import Data.Map (elems)

-- |type alias so we can choose between Int and Integer level precision easily
type IntType = Integer

-- |Helper function to get the int value of the sqrt of a number
upperBound :: Integral a => a -> a
upperBound = floor . sqrt . fromRational . toRational

-- |Returns a list of all the potential prime numbers below some target value
potentialPrimesList :: IntType   -- ^ Largest number to check for prime status
                    -> [IntType] -- ^ List of all the odd numbers smaller than the target
potentialPrimesList target = C.filter odd [3..upperBound target]

-- |Returns a SkipList of all the potential prime numbers below some target value
potentialPrimesSkipList :: IntType                      -- ^ Largest number to check for prime status
                     -> STM (TSkipList IntType IntType) -- ^ List of potentially prime numbers
potentialPrimesSkipList target = do
    x <- new 0.5 5
    sequence_ [ SL.insert i i x | i <- potentialPrimesList target]
    return x

main :: IO ()
main = do
    (target, numberOfThreads) <- readArgs -- Get the command line arguments for highest number to check and number of workers to spawn
    output <- startWorkers target numberOfThreads -- Start the workers
    sourceTBMChan output $$ CL.mapM_ (liftIO . putStrLn . show) -- Print prime numbers we get back

-- |Start all the worker threads and create the channels for passing data
startWorkers :: IntType              -- ^ Largest number we want to check
             -> Int                  -- ^ How many workers to spawn
             -> IO (TBMChan IntType) -- ^ Output channel that prime numbers will be written to
startWorkers target numberOfThreads = do
    (potentialPrimes, inputs, output) <- atomically $ do
        a <- potentialPrimesSkipList target
        b <- newTBMChan 100
        c <- newTBMChan 100
        return (a,b,c)
    C.mapM_ (\_ -> spawnWorker potentialPrimes inputs output) [1..numberOfThreads]
    forkIO $ primeCandidates target $$ sinkTBMChan inputs -- Feed numbers to the workers
    return output

-- |Concurrent producer to generate all the potential prime numbers we want to check.
-- Generates all odd numbers from 3 to the target number.
primeCandidates :: IntType              -- ^ Number to stop at
                -> Producer IO IntType  -- ^ Producer that generates the numbers
primeCandidates target = unfold (\a -> if a > target then Nothing else Just (a,a+2)) 3

-- |Spawns a worker thread to check if some set of numbers contains primes
spawnWorker :: TSkipList IntType IntType -- ^ Contains all the potential primes we need to check against
            -> TBMChan IntType           -- ^ Input channel we read potential primes from
            -> TBMChan IntType           -- ^ Output channel we write primes we find to
            -> IO ThreadId               -- ^ The ThreadId of this worker thread
spawnWorker checks inputs output = forkIO $ sourceTBMChan inputs
    $$ CL.mapMaybeM isPrime =$ sinkTBMChan output
    where
        isPrime :: IntType -> IO (Maybe IntType)
        isPrime x = do
            possiblePrimes <- atomically $ leq (upperBound x) checks -- Get all the potential primes below sqrt(target)
            if all (check x) (elems possiblePrimes) then return (Just x) else -- This number is prime, return it
                atomically (SL.delete x checks) >> return Nothing -- This number isn't prime, remove it from the list of numbers to check against
        check :: IntType -> IntType -> Bool
        check x y = x `rem` y /= 0
