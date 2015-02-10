package org.embulk.output.jdbc;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class RetryExecutor
{
    public static RetryExecutor retryExecutor()
    {
        return new RetryExecutor();
    }

    public static abstract class IdempotentOperation<T> implements Callable<T>
    {
        public abstract T call() throws Exception;

        public void onRetry(Throwable exception, int retryCount, int retryLimit, int retryWait)
        { }

        public void onGiveup(Throwable firstException, Throwable lastException)
        { }

        public abstract boolean isRetryableException(Throwable exception);
    }

    private int retryLimit = 3;
    private int initialRetryWait = 500;
    private int maxRetryWait = 30*60*1000;

    private RetryExecutor()
    { }

    public RetryExecutor setRetryLimit(int count)
    {
        this.retryLimit = count;
        return this;
    }

    public RetryExecutor setInitialRetryWait(int msec)
    {
        this.initialRetryWait = msec;
        return this;
    }

    public RetryExecutor setMaxRetryWait(int msec)
    {
        this.maxRetryWait = msec;
        return this;
    }

    public <T> T runInterruptible(IdempotentOperation<T> op) throws InterruptedException, ExecutionException
    {
        return run(op, true);
    }

    public <T> T run(IdempotentOperation<T> op) throws ExecutionException
    {
        try {
            return run(op, false);
        } catch (InterruptedException ex) {
            throw new ExecutionException("Unexpected interruption", ex);
        }
    }

    private <T> T run(IdempotentOperation<T> op, boolean interruptible)
            throws InterruptedException, ExecutionException
    {
        int retryWait = initialRetryWait;
        int retryCount = 0;

        Throwable firstException = null;

        while(true) {
            try {
                return op.call();
            } catch (Throwable exception) {
                if (firstException == null) {
                    firstException = exception;
                }
                if (!op.isRetryableException(exception) || retryCount >= retryLimit) {
                    op.onGiveup(firstException, exception);
                    throw new ExecutionException(firstException);
                }

                retryCount++;
                op.onRetry(exception, retryCount, retryLimit, retryWait);

                try {
                    Thread.sleep(retryWait);
                } catch (InterruptedException ex) {
                    if (interruptible) {
                        throw ex;
                    }
                }

                retryWait *= 2;  // exponential back-off

                if (retryWait > maxRetryWait) {
                    retryWait = maxRetryWait;
                }
            }
        }
    }
}

