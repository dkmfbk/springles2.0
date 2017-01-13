package eu.fbk.dkm.springles.base;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.impl.GraphQueryResultImpl;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;

import eu.fbk.dkm.internal.util.Iterations;
import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;

/**
 * A <tt>Transaction</tt> decorator handling operation synchronization and transaction
 * auto-commit, auto-closure, maximum execution and idle times.
 * <p>
 * This class intercept the start and end of each transaction operation to enforce five aspects:
 * <ul>
 * <li>Operation synchronization. At most one write operation can be active at any time, while
 * multiple read operations can be active, provided no other write operation is pending.
 * Synchronization is enforced using a read/write lock mechanism not tied to the thread currently
 * executing an operation: this permits to terminate read operations when the returned iteration
 * object is closed, perhaps in a different thread.</li>
 * <li>Auto-commit. Auto-commit is enforced by ending the transaction after the first issued
 * operation completes; the class also makes sure that at most one operation is started in
 * auto-commit mode.</li>
 * <li>Auto-closure. In auto-closure mode, the class ensures that closure is up-to-date before
 * executing read operations (or write operations that also read data) that ask for forward
 * reasoning. Also, closure is updated when the transaction ends.</li>
 * <li>Maximum execution and idle times. If specified at construction time, these time limits are
 * enforced by scheduling a 'watchdog' object for periodic execution (using a supplied scheduler):
 * each time it is activated, it checks whether timeout has occurred, and rolls back the
 * transaction if it is the case.</li>
 * </ul>
 * </p>
 */
final class SynchronizedTransaction extends ForwardingTransaction
{

    // Implementation note: two-level synchronization: a semaphore is used for controlling
    // read/write access; synchronized blocks are used just to ensure all registered iterations
    // are closed.

    /**
     * Enumeration of possible transaction states.
     */
    public enum TransactionStatus
    {
        /** Active transaction. */
        ACTIVE,

        /** Method {@link Transaction#end(boolean)} has been called, but have to complete. */
        ENDING,

        /**
         * Transaction has ended due to method end() being called explicitly or as part of
         * auto-commit.
         */
        ENDED,

        /** Transaction has ended due to a timeout. */
        TIMEDOUT
    }

    /**
     * Enumeration of possible states of threads busy in executing transaction operations.
     */
    public enum ThreadStatus
    {

        /** Thread is executing a transaction operation different from <tt>execute()</tt>. */
        OUTSIDE_EXECUTE,

        /** Thread is executing an <tt>execute()</tt> read operation. */
        INSIDE_READ_EXECUTE,

        /** Thread is executing an <tt>execute()</tt> write operation. */
        INSIDE_WRITE_EXECUTE
    }

    /**
     * Interface of listeners notified when a transaction ends.
     */
    public interface EndListener
    {

        /**
         * Callback method invoked when the transaction for which the listener has been supplied
         * ends.
         * 
         * @param committed
         *            <tt>true</tt>, if the transaction was committed
         * @param newClosureStatus
         *            the closure status resulting from the execution of the transaction
         */
        void transactionEnded(boolean committed, ClosureStatus newClosureStatus);

    }

    /** Shared log object. */
    private static final Logger LOGGER = LoggerFactory.getLogger(SynchronizedTransaction.class);

    /** The time period between consecutive activations of the 'watchdog'. */
    private static final long WATCHDOG_PERIOD = 1000L;

    /** The maximum number of concurrent transaction operations. */
    private static final int MAX_CONCURRENT_OPERATIONS = 256; // a large enough number :-)

    /** The transaction this wrapper delegates to. */
    private final Transaction delegate;

    /** Flag being <tt>true</tt> if the wrapped transaction is writable. */
    private final boolean writable;

    /** Flag being <tt>true</tt> if auto-commit must be enforced. */
    private final boolean autoCommit;

    /** Flag being <tt>true</tt> if auto-closure must be enforced. */
    private final boolean autoClosure;

    /** The optional listener to be notified when the transaction ends. */
    @Nullable
    private final EndListener listener;

    /** A semaphore objects used for read/write synchronization. */
    private final Semaphore semaphore; // unfair mode (expect low contention)

    /** A set of pending iterations returned by the transaction, to end if the transaction ends. */
    private final Set<CloseableIteration<?, ?>> pendingIterations;

    /** A flag being <tt>true</tt> if closure may be not up-to-date. */
    private volatile boolean checkClosure; // unsynchronized: updateClosure is nop if not needed

    /** A flag being <tt>true</tt> if further operations must be rejected (for auto-commit mode). */
    private volatile boolean rejectStartOperation;

    /** A thread local variable storing the states of thread busy in transaction operations. */
    private final ThreadLocal<ThreadStatus> threadStatus;

    /** The transaction status. */
    private volatile TransactionStatus transactionStatus;

    /** The optional watchdog, activated periodically to check and react for transaction timeout. */
    @Nullable
    private Watchdog watchdog;

    /**
     * Creates a new instance wrapping the transaction supplied, not enforcing maximum idle and
     * execution times.
     * 
     * @param delegate
     *            the wrapped transaction
     * @param writable
     *            <tt>true</tt> if the wrapped transaction is writable
     * @param autoCommit
     *            <tt>true</tt> if auto-commit must be enforced
     * @param autoClosure
     *            <tt>true</tt> if auto-closure must be enforced (transaction must be writable)
     * @param listener
     *            an optional listener notified when the transaction ends
     */
    public SynchronizedTransaction(final Transaction delegate, final boolean writable,
            final boolean autoCommit, final boolean autoClosure,
            @Nullable final EndListener listener)
    {
        Preconditions.checkNotNull(delegate);

        this.delegate = delegate;
        this.writable = writable;
        this.autoCommit = autoCommit;
        this.autoClosure = autoClosure;
        this.listener = listener;
        this.semaphore = new Semaphore(MAX_CONCURRENT_OPERATIONS, true);
        this.pendingIterations = Sets.newHashSet();
        this.checkClosure = autoClosure; // force checking at first operation
        this.rejectStartOperation = false;
        this.threadStatus = new ThreadLocal<ThreadStatus>() {

            @Override
            protected ThreadStatus initialValue()
            {
                return ThreadStatus.OUTSIDE_EXECUTE;
            }

        };
        this.transactionStatus = TransactionStatus.ACTIVE;
    }

    /**
     * Creates a new instance wrapping the transaction supplied and enforcing the maximum
     * execution and idle times.
     * 
     * @param delegate
     *            the wrapped transaction
     * @param writable
     *            <tt>true</tt> if the wrapped transaction is writable
     * @param autoCommit
     *            <tt>true</tt> if auto-commit must be enforced
     * @param autoClosure
     *            <tt>true</tt> if auto-closure must be enforced (transaction must be writable)
     * @param listener
     *            an optional listener notified when the transaction ends
     * @param maxExecutionTime
     *            the maximum transaction execution time, in ms, measured from the time this
     *            object is created; negative or zero if maximum execution time should not be
     *            checked
     * @param maxIdleTime
     *            the maximum transaction idle time, in ms, measured from the time this object is
     *            created or was busy in an operation; negative or zero if maximum idle time
     *            should not be checked
     * @param scheduler
     *            the scheduler object required to enforce max execution and idle times
     */
    public SynchronizedTransaction(final Transaction delegate, final boolean writable,
            final boolean autoCommit, final boolean autoClosure,
            @Nullable final EndListener listener, final long maxExecutionTime,
            final long maxIdleTime, final ScheduledExecutorService scheduler)
    {
        this(delegate, writable, autoCommit, autoClosure, listener);
        this.watchdog = new Watchdog(maxExecutionTime, maxIdleTime, scheduler);
    }

    /**
     * {@inheritDoc} Returns the delegate transaction object set at construction time.
     */
    @Override
    protected Transaction delegate()
    {
        return this.delegate;
    }

    // OPERATION SYNCHRONIZATION AND AUTO-COMMIT SUPPORT

    /**
     * Method called each time a read operation starts. The method performs three functions: (1)
     * it checks whether the operation can be actually performed, based on the {@link #autoCommit}
     * property and transaction status, throwing an exception if execution is not possible; (2) it
     * enforces proper operation synchronization, ensuring a read operation is not started if a
     * write operation is in progress; and (3) it ensures the closure is up-to-date before
     * starting the operation, if <tt>closureNeeded</tt> is <tt>true</tt>.
     * 
     * @param closureNeeded
     *            <tt>true</tt> if closure must be up-to-date before starting the operation
     * @throws RepositoryException
     *             in case closure update fails or the thread is interrupted while waiting to
     *             acquire a read lock
     */
    private void startReadOperation(final boolean closureNeeded) throws RepositoryException
    {
        switch (this.threadStatus.get()) {
        case OUTSIDE_EXECUTE:
            if (this.rejectStartOperation || this.transactionStatus != TransactionStatus.ACTIVE) {
                startOperationFailed();
            }
            try {
                if (this.checkClosure && closureNeeded) {
                    this.semaphore.acquire(MAX_CONCURRENT_OPERATIONS);
                    try {
                        if (this.transactionStatus != TransactionStatus.ACTIVE) {
                            startOperationFailed();
                        }
                        delegate().updateClosure();
                        this.checkClosure = false;
                    } finally {
                        if (!this.checkClosure) { // success
                            this.semaphore.release(MAX_CONCURRENT_OPERATIONS - 1);
                        } else { // failure
                            this.semaphore.release(MAX_CONCURRENT_OPERATIONS);
                        }
                    }
                } else {
                    this.semaphore.acquire();
                }
            } catch (final InterruptedException ex) {
                throw new RepositoryException("Thread interrupted while acquiring lock to "
                        + "start a read operation");
            }
            if (this.transactionStatus != TransactionStatus.ACTIVE) {
                this.semaphore.release();
                startOperationFailed();
            }
            this.rejectStartOperation |= this.autoCommit;
            break;

        case INSIDE_READ_EXECUTE:
        case INSIDE_WRITE_EXECUTE:
            if (this.checkClosure && closureNeeded) {
                delegate().updateClosure();
                this.checkClosure = false;
                if (this.transactionStatus != TransactionStatus.ACTIVE) {
                    startOperationFailed();
                }
            }
            break;

        default:
            throw new Error();
        }
    }

    /**
     * Method called each time a write operation starts. The method performs three functions: (1)
     * it checks whether the operation can be actually performed, based on the {@link #autoCommit}
     * property and transaction status, throwing an exception if execution is not possible; (2) it
     * enforces proper operation synchronization, ensuring the write operation starts when no
     * other operation is in progress; and (3) it ensures the closure is up-to-date before
     * starting the operation, if <tt>closureNeeded</tt> is <tt>true</tt>.
     * 
     * @param closureNeeded
     *            <tt>true</tt> if closure must be up-to-date before starting the operation
     * @throws RepositoryException
     *             in case closure update fails or the thread is interrupted while waiting to
     *             acquire a read lock
     */
    private void startWriteOperation(final boolean closureNeeded) throws RepositoryException
    {
        if (!this.writable) {
            throw new IllegalStateException(
                    "Cannot issue a write operation in a read-only transaction");
        }

        switch (this.threadStatus.get()) {
        case OUTSIDE_EXECUTE:
            if (this.rejectStartOperation || this.transactionStatus != TransactionStatus.ACTIVE) {
                startOperationFailed();
            }
            try {
                this.semaphore.acquire(MAX_CONCURRENT_OPERATIONS);
                boolean success = false;
                try {
                    if (this.transactionStatus != TransactionStatus.ACTIVE) {
                        startOperationFailed();
                    }
                    if (this.checkClosure && closureNeeded) {
                        delegate().updateClosure();
                        this.checkClosure = false;
                        if (this.transactionStatus != TransactionStatus.ACTIVE) {
                            startOperationFailed();
                        }
                    }
                    success = true;
                } finally {
                    if (!success) {
                        this.semaphore.release(MAX_CONCURRENT_OPERATIONS);
                    }
                }

            } catch (final InterruptedException ex) {
                throw new RepositoryException("Thread interrupted while acquiring lock to "
                        + "start a read operation");
            }
            this.rejectStartOperation |= this.autoCommit;
            break;

        case INSIDE_READ_EXECUTE:
            throw new IllegalStateException("Cannot perform a write operation "
                    + "when calling execute() with a read-only operation");

        case INSIDE_WRITE_EXECUTE:
            if (this.checkClosure && closureNeeded) {
                delegate().updateClosure();
                this.checkClosure = false;
                if (this.transactionStatus != TransactionStatus.ACTIVE) {
                    startOperationFailed();
                }
            }
            break;

        default:
            throw new Error();
        }
    }

    /**
     * Helper method that throws a suitable exception with an explaining message in case the start
     * of an operation failed.
     */
    private void startOperationFailed()
    {
        switch (this.transactionStatus) {
        case ENDING:
            throw new IllegalStateException("Transaction is being ended while acquiring lock "
                    + "to start a new operation. Operation aborted.");
        case ENDED:
            throw new IllegalStateException("Transaction already ended. "
                    + "Cannot start new operations.");
        case TIMEDOUT:
            throw new IllegalStateException("Transaction has timed out. "
                    + "Cannot start new operation");
        default:
            if (this.rejectStartOperation) {
                throw new Error("Cannot start more than one operation "
                        + "within the same transaction in auto-commit mode");
            }
            throw new Error();
        }
    }

    /**
     * Method called each time a read operation ends. The method releases the shared read lock
     * associated to the operation, performs auto-commit if required and updates the
     * {@link #watchdog} associated to the operation.
     * 
     * @throws RepositoryException
     *             in case auto-commit fails
     */
    private void endReadOperation() throws RepositoryException
    {
        if (this.watchdog != null) {
            this.watchdog.touch();
        }
        if (this.threadStatus.get() == ThreadStatus.OUTSIDE_EXECUTE) {
            this.semaphore.release();
            if (this.autoCommit) {
                end(true);
                LOGGER.debug("[{}] Transaction automatically committed", getID());
            }
        }
    }

    /**
     * Method called each time a write operation ends. The method releases the exclusive lock
     * associated to the operation, performs auto-commit if required and updates the
     * {@link #watchdog} associated to the operation.
     * 
     * @param operationSucceeded
     *            in auto-commit mode, controls whether a commit or a rollback must occur
     * @throws RepositoryException
     *             in case auto-commit fails
     */
    private void endWriteOperation(final boolean operationSucceeded) throws RepositoryException
    {
        if (this.watchdog != null) {
            this.watchdog.touch();
        }
        if (this.threadStatus.get() == ThreadStatus.OUTSIDE_EXECUTE) {
            this.semaphore.release(MAX_CONCURRENT_OPERATIONS);
            if (this.autoCommit) {
                end(operationSucceeded);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[{}] Transaction automatically {}", getID(),
                            operationSucceeded ? "committed" : "rolled-back");
                }
            }
        }
    }

    // ITERATION MANAGEMENT

    /**
     * Forces closure of all pending iterations.
     */
    private synchronized void closeIterations()
    {
        for (final CloseableIteration<?, ?> iteration : ImmutableList
                .copyOf(this.pendingIterations)) {
            Iterations.closeQuietly(iteration);
        }
    }

    /**
     * Method called each time an iteration is returned by the transaction. In case the
     * transaction is ending/ended, the iteration is terminated immediately; otherwise (if outside
     * an <tt>execute()</tt> call), the iteration is registered as pending and is wrapped so to be
     * able to unregister it or force its termination when the transaction ends.
     * 
     * @param iteration
     *            the intercepted iteration returned by the transaction
     * @return the supplied iteration, when possible, or a wrapper of if
     */
    private synchronized <T> CloseableIteration<? extends T, RepositoryException> wrap(
            final CloseableIteration<? extends T, RepositoryException> iteration)
    {
        if (this.transactionStatus != TransactionStatus.ACTIVE) {
            LOGGER.debug("[{}] Forcing closure of iteration as transaction is not active", getID());
            Iterations.closeQuietly(iteration);
            return iteration;

        } else if (this.threadStatus.get() != ThreadStatus.OUTSIDE_EXECUTE) {
            return iteration;

        } else {
            final RepositoryResult<T> wrappedIteration = new RepositoryResult<T>(iteration) {

                @Override
                protected void handleClose() throws RepositoryException
                {
                    try {
                        super.handleClose();
                    } finally {
                        SynchronizedTransaction.this.pendingIterations.remove(iteration);
                        endReadOperation();
                    }
                }

            };
            this.pendingIterations.add(wrappedIteration);
            return wrappedIteration;
        }
    }

    /**
     * Method called each time a tuple result is returned by the transaction. The method performs
     * similarly to {@link #wrap(CloseableIteration)}.
     */
    private synchronized TupleQueryResult wrap(final TupleQueryResult iteration) throws QueryEvaluationException
    {
        if (this.transactionStatus != TransactionStatus.ACTIVE) {
            LOGGER.debug("[{}] Forcing closure of iteration as transaction is not active", getID());
            Iterations.closeQuietly(iteration);
            return iteration;

        } else if (this.threadStatus.get() != ThreadStatus.OUTSIDE_EXECUTE) {
            return iteration;

        } else {
            TupleQueryResult wrappedIteration = new TupleQueryResultImpl(
        	//final MutableTupleQueryResult wrappedIteration = new MutableTupleQueryResult(
                   iteration.getBindingNames(), iteration) {

               @Override
                protected void handleClose() throws QueryEvaluationException
                {
                    try {
                        super.handleClose();
                    } finally {
                        try {
                            SynchronizedTransaction.this.pendingIterations.remove(iteration);
                            endReadOperation();
                        } catch (final RepositoryException ex) {
                         
                        }
                    }
                }

            };
            this.pendingIterations.add(wrappedIteration);
            return wrappedIteration;
        }
    }

    /**
     * Method called each time a graph result is returned by the transaction. The method performs
     * similarly to {@link #wrap(CloseableIteration)}.
     * @throws QueryEvaluationException 
     */
    private synchronized GraphQueryResult wrap(final GraphQueryResult iteration) throws QueryEvaluationException
    {
        if (this.transactionStatus != TransactionStatus.ACTIVE) {
            LOGGER.debug("[{}] Forcing closure of iteration as transaction is not active", getID());
            Iterations.closeQuietly(iteration);
            return iteration;

        } else if (this.threadStatus.get() != ThreadStatus.OUTSIDE_EXECUTE) {
            return iteration;

        } else {
            final GraphQueryResult wrappedIteration = new GraphQueryResultImpl(
                    iteration.getNamespaces(), iteration) {

                @Override
                protected void handleClose() throws QueryEvaluationException
                {
                    try {
                        super.handleClose();
                    } finally {
                        try {
                            SynchronizedTransaction.this.pendingIterations.remove(iteration);
                            endReadOperation();
                        } catch (final RepositoryException ex) {
                            throw new QueryEvaluationException(ex);
                        }
                    }
                }

            };
            this.pendingIterations.add(wrappedIteration);
            return wrappedIteration;
        }
    }

    /**
     * Wraps an object if it is an iteration, calling the appropriate <tt>wrap</tt> method.
     * 
     * @param object
     *            the object that has to be possibly wrapped
     * @return the object itself or a wrapper of it
     * @throws QueryEvaluationException 
     */
    @SuppressWarnings("unchecked")
    private <T> T wrapIfNecessary(@Nullable final T object) throws QueryEvaluationException
    {
        if (object instanceof TupleQueryResult) {
            return (T) wrap((TupleQueryResult) object);
        } else if (object instanceof GraphQueryResult) {
            return (T) wrap((GraphQueryResult) object);
        } else if (object instanceof CloseableIteration<?, ?>) {
            return (T) wrap((CloseableIteration<?, RepositoryException>) object);
        } else {
            return object;
        }
    }

    // TRANSACTION API METHODS

    /**
     * {@inheritDoc} Delegates in the context of a read operation with no closure requirement.
     */
    @Override
    public String getNamespace(final String prefix) throws RepositoryException
    {
        startReadOperation(false);
        try {
            return delegate().getNamespace(prefix);
        } finally {
            endReadOperation();
        }
    }

    /**
     * {@inheritDoc} Delegates in the context of a read operation with no closure requirement.
     */
    @Override
    public CloseableIteration<? extends Namespace, RepositoryException> getNamespaces()
            throws RepositoryException
    {
        startReadOperation(false);
        CloseableIteration<? extends Namespace, RepositoryException> result = null;
        try {
            result = wrap(delegate().getNamespaces());
            return result;
        } finally {
            if (result == null) {
                endReadOperation();
            }
        }
    }

    /**
     * {@inheritDoc} Delegates in the context of a write operation with no closure requirement.
     */
    @Override
    public void setNamespace(final String prefix, @Nullable final String name)
            throws RepositoryException
    {
        startWriteOperation(false);
        boolean success = false;
        try {
            delegate().setNamespace(prefix, name);
            success = true;
        } finally {
            endWriteOperation(success);
        }
    }

    /**
     * {@inheritDoc} Delegates in the context of a write operation with no closure requirement.
     */
    @Override
    public void clearNamespaces() throws RepositoryException
    {
        startWriteOperation(false);
        boolean success = false;
        try {
            delegate().clearNamespaces();
            success = true;
        } finally {
            endWriteOperation(success);
        }
    }

    /**
     * {@inheritDoc} Executes as a read operation with closure updated if forward inference is on.
     */
    @Override
    public void query(final QuerySpec<?> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode, final int timeout,
            final Object handler) throws QueryEvaluationException, RepositoryException
    {
        startReadOperation(mode.isForwardEnabled());
        try {
            delegate().query(query, dataset, bindings, mode, timeout, handler);

        } finally {
            endReadOperation();
        }
    }

    /**
     * {@inheritDoc} Executes as a read operation with closure updated if forward inference is on.
     * If a tuple or query result is returned, they are wrapped so that they must be closed for
     * the operation to complete.
     */
    @Override
    public <T> T query(final QuerySpec<T> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode, final int timeout)
            throws QueryEvaluationException, RepositoryException
    {
        startReadOperation(mode.isForwardEnabled());

        boolean success = false;
        try {
            final T result = wrapIfNecessary(delegate().query(query, dataset, bindings, mode,
                    timeout));
            success = true;
            return result;

        } finally {
            if (!success || query.getType() == QueryType.BOOLEAN) {
                endReadOperation();
            }
        }
    }

    /**
     * {@inheritDoc} Executes as a read operation with closure updated if forward inference is on.
     * If a tuple or query result is returned, they are wrapped so that they must be closed for
     * the operation to complete.
     */
    @Override
    public <T> T query(final URI queryURI, final QueryType<T> queryType, final InferenceMode mode,
            final Object... parameters) throws QueryEvaluationException, RepositoryException
    {
        startReadOperation(mode.isForwardEnabled());

        boolean success = false;
        try {
            final T result = wrapIfNecessary(delegate().query(queryURI, queryType, mode,
                    parameters));
            success = true;
            return result;

        } finally {
            if (!success || queryType == QueryType.BOOLEAN) {
                endReadOperation();
            }
        }
    }

    /**
     * {@inheritDoc} Executes as a read operation with closure updated if forward inference is on.
     * The returned iteration is wrapped and must be closed for the operation to complete.
     */
    @Override
    public CloseableIteration<? extends Resource, RepositoryException> getContextIDs(
            final InferenceMode mode) throws RepositoryException
    {
        startReadOperation(mode.isForwardEnabled());
        CloseableIteration<? extends Resource, RepositoryException> result = null;
        try {
            result = wrap(delegate().getContextIDs(mode));
            return result;
        } finally {
            if (result == null) {
                endReadOperation();
            }
        }
    }

    /**
     * {@inheritDoc} Executes as a read operation with closure updated if forward inference is on.
     * The returned iteration is wrapped and must be closed for the operation to complete.
     */
    @Override
    public CloseableIteration<? extends Statement, RepositoryException> getStatements(
            @Nullable final Resource subj, @Nullable final URI pred, @Nullable final Value obj,
            final InferenceMode mode, final Resource... contexts) throws RepositoryException
    {
        startReadOperation(mode.isForwardEnabled());
        CloseableIteration<? extends Statement, RepositoryException> result = null;
        try {
            result = wrap(delegate().getStatements(subj, pred, obj, mode, contexts));
            return result;
        } finally {
            if (result == null) {
                endReadOperation();
            }
        }
    }

    /**
     * {@inheritDoc} Executes as a read operation with closure updated if forward inference is on.
     */
    @Override
    public boolean hasStatement(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final InferenceMode mode, final Resource... contexts)
            throws RepositoryException
    {
        startReadOperation(mode.isForwardEnabled());
        try {
            return delegate().hasStatement(subj, pred, obj, mode, contexts);
        } finally {
            endReadOperation();
        }
    }

    /**
     * {@inheritDoc} Executes as a read operation with closure updated if forward inference is on.
     */
    @Override
    public long size(final InferenceMode mode, final Resource... contexts)
            throws RepositoryException
    {
        startReadOperation(mode.isForwardEnabled());
        try {
            return delegate().size(mode, contexts);
        } finally {
            endReadOperation();
        }
    }

    /**
     * {@inheritDoc} Executes as a write operation; closure is updated if forward inference is on.
     */
    @Override
    public void update(final UpdateSpec update, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode)
            throws UpdateExecutionException, RepositoryException
    {
        startWriteOperation(mode.isForwardEnabled());
        boolean success = false;
        try {
            this.checkClosure = this.autoClosure;
            delegate().update(update, dataset, bindings, mode);
            success = true;
        } finally {
            endWriteOperation(success);
        }
    }

    /**
     * {@inheritDoc} Executes as a write operation; closure is updated if forward inference is on.
     */
    @Override
    public void update(final URI updateURI, final InferenceMode mode, final Object... parameters)
            throws UpdateExecutionException, RepositoryException
    {
        startWriteOperation(mode.isForwardEnabled());
        boolean success = false;
        try {
            this.checkClosure = this.autoClosure;
            delegate().update(updateURI, mode, parameters);
            success = true;
        } finally {
            endWriteOperation(success);
        }
    }

    /**
     * {@inheritDoc} Executes as a write operation with no closure requirement.
     */
    @Override
    public void add(final Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException
    {
        startWriteOperation(false);
        boolean success = false;
        try {
            this.checkClosure = this.autoClosure;
            delegate().add(statements, contexts);
            success = true;
        } finally {
            endWriteOperation(success);
        }
    }

    /**
     * {@inheritDoc} Executes as a write operation with no closure requirement.
     */
    @Override
    public void remove(final Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException
    {
        startWriteOperation(false);
        boolean success = false;
        try {
            this.checkClosure = this.autoClosure;
            delegate().remove(statements, contexts);
            success = true;
        } finally {
            endWriteOperation(success);
        }
    }

    /**
     * {@inheritDoc} Executes as a write operation with no closure requirement.
     */
    @Override
    public void remove(@Nullable final Resource subject, @Nullable final URI predicate,
            @Nullable final Value object, final Resource... contexts) throws RepositoryException
    {
        startWriteOperation(false);
        boolean success = false;
        try {
            this.checkClosure = this.autoClosure;
            delegate().remove(subject, predicate, object, contexts);
            success = true;
        } finally {
            endWriteOperation(success);
        }
    }

    /**
     * {@inheritDoc} Executes as a read operation with no closure requirement.
     */
    @Override
    public ClosureStatus getClosureStatus() throws RepositoryException
    {
        startReadOperation(false);
        try {
            return delegate().getClosureStatus();
        } finally {
            endReadOperation();
        }
    }

    /**
     * {@inheritDoc} Executes as a write operation with no closure requirement (it will already
     * manipulate the closure).
     */
    @Override
    public void updateClosure() throws RepositoryException
    {
        startWriteOperation(false);
        boolean success = false;
        try {
            delegate().updateClosure();
            this.checkClosure = this.autoClosure;
            success = true;
        } finally {
            endWriteOperation(success);
        }
    }

    /**
     * {@inheritDoc} Executes as a write operation with no closure requirement (it will already
     * manipulate the closure).
     */
    @Override
    public void clearClosure() throws RepositoryException
    {
        startWriteOperation(false);
        boolean success = false;
        try {
            this.checkClosure = this.autoClosure;
            delegate().clearClosure();
            success = true;
        } finally {
            endWriteOperation(success);
        }
    }

    /**
     * {@inheritDoc} Executes as a write operation with no closure requirement (closure is deleted
     * as part of the operation).
     */
    @Override
    public void reset() throws RepositoryException
    {
        startWriteOperation(false);
        boolean success = false;
        try {
            this.checkClosure = this.autoClosure;
            delegate().reset();
            success = true;
        } finally {
            endWriteOperation(success);
        }
    }

    /**
     * {@inheritDoc} Executes as a read or write operation, with or without closure requirements,
     * depending on the parameters supplied.
     */
    @Override
    public <T, E extends Exception> T execute(final Operation<T, E> operation,
            final boolean writeOperation, final boolean closureNeeded) throws E,
            RepositoryException
    {
        if (writeOperation) {
            startWriteOperation(closureNeeded);
        } else {
            startReadOperation(closureNeeded);
        }

        boolean success = false;
        try {
            this.threadStatus.set(writeOperation ? ThreadStatus.INSIDE_WRITE_EXECUTE
                    : ThreadStatus.INSIDE_READ_EXECUTE);
            T result = delegate().execute(operation, writeOperation, closureNeeded);
            this.threadStatus.set(ThreadStatus.OUTSIDE_EXECUTE);
            result = wrapIfNecessary(result);
            success = true;
            return result;

        } catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} finally {
            this.threadStatus.set(ThreadStatus.OUTSIDE_EXECUTE);
            if (writeOperation) {
                endWriteOperation(success);
            } else {
                endReadOperation();
            }
        }
		
    }

    /**
     * {@inheritDoc} The method awaits for the termination of pending operation, forcing the
     * closure of active iterations and preventing new iterations from being started. Closure is
     * then updated if auto-closure is on, and the transaction is then ended by delegating. If an
     * end listener was supplied at construction time, it is notified.
     */
    @Override
    public void end(final boolean commit) throws RepositoryException
    {
        synchronized (this) {
            if (this.transactionStatus != TransactionStatus.ACTIVE) {
                return; // BEWARE: don't wait for end() to complete.
            }
            this.transactionStatus = TransactionStatus.ENDING;
            closeIterations();
        }

        ClosureStatus newClosureStatus = null;
        boolean committed = false;
        boolean actualCommit = commit;

        this.semaphore.acquireUninterruptibly(MAX_CONCURRENT_OPERATIONS);
        try {
            try {
                if (this.checkClosure) {
                    actualCommit = false;
                    delegate().updateClosure();
                    this.checkClosure = false;
                    actualCommit = commit;
                }
            } finally {
                newClosureStatus = delegate().getClosureStatus();
                delegate().end(actualCommit);
                committed = actualCommit;
            }

        } finally {
            this.transactionStatus = TransactionStatus.ENDED;
            this.semaphore.release(MAX_CONCURRENT_OPERATIONS); // allows pending calls to complete
            if (this.listener != null) {
                try {
                    this.listener.transactionEnded(committed, newClosureStatus);
                } catch (final Throwable ex) {
                    LOGGER.warn("Unexpected exception thrown by listener. Ignoring.", ex);
                }
            }
            this.threadStatus.set(null); // avoid memory leaks
        }
    }

    /**
     * Helper class supporting the enforcement of the maximum transaction execution and idle
     * times.
     * <p>
     * When a {@link SynchronizedTransaction} is created suppling maximum execution and idle
     * times, an instance of this class is registered with the supplied scheduler to be executed
     * (being a <tt>Runnable</tt>) periodically. Every time it invoked, method {@link #run()}
     * checks whether the maximum execution and idle time have expired: if it is the case, the
     * transaction is rolled back. In order to track idle time, method {@link #touch()} has to be
     * called each time the transaction is busy in an operation, so to update the last activity
     * timestamp.
     * </p>
     */
    private class Watchdog implements Runnable
    {

        /** The configured maximum execution time, in ms. */
        private final long maxExecutionTime;

        /** The configured maximum idle time, in ms. */
        private final long maxIdleTime;

        /** The transaction start timestamp, in ms. */
        private final long startTime;

        /** The transaction last activity timestamp, in ms. */
        private long idleTime;

        /** A future object used to unregister the watchdog from the scheduler. */
        private final ScheduledFuture<?> future;

        /**
         * Creates a new instance for the maximum times specified, and register it with the
         * supplied scheduler.
         * 
         * @param maxExecutionTime
         *            the maximum transaction execution time, in ms
         * @param maxIdleTime
         *            the maximum transaction idle time, in ms
         * @param scheduler
         *            the scheduler where to schedule the watchdog
         */
        public Watchdog(final long maxExecutionTime, final long maxIdleTime,
                final ScheduledExecutorService scheduler)
        {
            this.maxExecutionTime = maxExecutionTime;
            this.maxIdleTime = maxIdleTime;
            this.startTime = System.currentTimeMillis();
            this.idleTime = this.startTime;
            this.future = scheduler.scheduleAtFixedRate(this, WATCHDOG_PERIOD, WATCHDOG_PERIOD,
                    TimeUnit.MILLISECONDS);
        }

        /**
         * Updates the transaction last activity timestamp.
         */
        public void touch()
        {
            this.idleTime = System.currentTimeMillis();
        }

        /**
         * Checks whether the transaction time out, w.r.t. the maximum execution and idle times.
         * 
         * @return true if transaction timeout occurred
         */
        public boolean timeout()
        {
            final SynchronizedTransaction tx = SynchronizedTransaction.this;
            final long time = System.currentTimeMillis();
            return this.maxExecutionTime > 0 && time - this.startTime > this.maxExecutionTime
                    || this.maxIdleTime > 0 && time - this.idleTime > this.maxIdleTime
                    && tx.transactionStatus == TransactionStatus.ACTIVE
                    && tx.semaphore.availablePermits() == MAX_CONCURRENT_OPERATIONS;
        }

        /**
         * {@inheritDoc} This method is executed periodically and terminates the transaction if
         * timeout is detected.
         */
        @Override
        public synchronized void run()
        {
            final SynchronizedTransaction tx = SynchronizedTransaction.this;

            if (timeout() && tx.transactionStatus == TransactionStatus.ACTIVE) {
                try {
                    tx.end(false);
                } catch (final Throwable ex) {
                    LOGGER.error("Exception caught while ending timed out transaction", ex);
                }
                if (tx.transactionStatus == TransactionStatus.ENDED) {
                    tx.transactionStatus = TransactionStatus.TIMEDOUT;
                }
                close();
            }
        }

        /**
         * Closes the watchdog, either after the transaction ends normally or timeout occurs.
         */
        public synchronized void close()
        {
            if (!this.future.isCancelled()) {
                this.future.cancel(false);
            }
        }

    }

}
