package eu.fbk.dkm.springles.base;

import org.slf4j.Logger;

public final class Transactions
{

    // TODO: add factory methods for ContextEnforcingTransaction, SynchronizedTransaction,
    // BufferedTransaction?

    public static Transaction unmodifiableTransaction(final Transaction delegate)
    {
        return delegate instanceof UnmodifiableTransaction ? delegate
                : new UnmodifiableTransaction(delegate);
    }

    public static Transaction debuggingTransaction(final Transaction delegate, final Logger logger)
    {
        return new DebuggingTransaction(delegate, logger);
    }

    private Transactions()
    {
    }

}
