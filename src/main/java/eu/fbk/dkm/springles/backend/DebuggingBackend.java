package eu.fbk.dkm.springles.backend;

import java.io.File;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;

import eu.fbk.dkm.springles.base.Transaction;
import eu.fbk.dkm.springles.base.Transactions;

/**
 * A <tt>Backend</tt> decorator that logs method calls and checks they respect the
 * <tt>Backend</tt> API and lifecycle.
 * <p>
 * This decorator is used for debugging purposes. It logs method invocations (at <tt>DEBUG</tt>
 * level) using a supplied {@link Logger} and checks preconditions concerning both method
 * parameters and invocation order w.r.t. the prescribed <tt>Backend</tt> lifecycle.
 * </p>
 */
final class DebuggingBackend extends ForwardingBackend
{

    /** The wrapped backend. */
    private final Backend delegate;

    /** The logger instance to be used. */
    private final Logger logger;

    /** Flag being <tt>true</tt> if the backend has been initialized. */
    private volatile boolean initialized;

    /** Flag being <tt>true</tt> if the backend has been closed. */
    private volatile boolean closed;

    /**
     * Creates a new wrapper instance using the wrapped backend and logger specified.
     *
     * @param delegate
     *            the wrapped backend
     * @param logger
     *            the logger
     */
    public DebuggingBackend(final Backend delegate, final Logger logger)
    {
        Preconditions.checkNotNull(delegate);
        Preconditions.checkNotNull(logger);

        this.delegate = delegate;
        this.logger = logger;
        this.initialized = false;
        this.closed = false;
    }

    /**
     * {@inheritDoc} Returns the wrapped backend set at construction time.
     */
    @Override
    protected Backend delegate()
    {
        return this.delegate;
    }

    /**
     * {@inheritDoc} Delegates and logs, checking the invocation respects the backend lifecycle.
     */
    @Override
    public void initialize(@Nullable final File dataDir) throws RepositoryException
    {
        Preconditions.checkState(!this.initialized);
        Preconditions.checkState(!this.closed);

        this.delegate.initialize(dataDir);
        this.initialized = true;

        if (this.logger.isInfoEnabled()) {
            this.logger.info("Backend initialized: data dir = " + dataDir + ", writable = "
                    + this.isWritable() + ", value factory = " + this.getValueFactory());
        }
    }

    /**
     * {@inheritDoc} Delegates, checking the invocation respects the backend lifecycle.
     */
    @Override
    public boolean isWritable()
    {
        Preconditions.checkState(this.initialized);
        Preconditions.checkState(!this.closed);

        return this.delegate.isWritable();
    }

    /**
     * {@inheritDoc} Delegates, checking the invocation respects the backend lifecycle.
     */
    @Override
    public ValueFactory getValueFactory()
    {
        Preconditions.checkState(this.initialized);
        Preconditions.checkState(!this.closed);

        return this.delegate.getValueFactory();
    }

    /**
     * {@inheritDoc} Delegates and logs, wrapping the returned transaction with
     * {@link Transactions#debuggingTransaction(Transaction, Logger)} and checking the invocation
     * respects the backend lifecycle.
     */
    @Override
    public Transaction newTransaction(final String id, final boolean writable)
            throws RepositoryException
    {
        Preconditions.checkNotNull(id);
        Preconditions.checkState(this.initialized);
        Preconditions.checkState(!this.closed);

        final Transaction transaction = this.delegate.newTransaction(id, writable);
        Preconditions.checkNotNull(transaction);
        this.logger.info("[{}] Backend transaction created, writable = {}", id, writable);

        return Transactions.debuggingTransaction(transaction, this.logger);
    }

    /**
     * {@inheritDoc} Delegates and logs, checking the invocation respects the backend lifecycle.
     */
    @Override
    public void close() throws RepositoryException
    {
        Preconditions.checkState(this.initialized);
        Preconditions.checkState(!this.closed);

        try {
            this.delegate.close();
            this.logger.info("Backend closed");
        } finally {
            this.closed = true;
        }
    }

}
