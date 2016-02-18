package eu.fbk.dkm.springles.base;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.dkm.internal.util.URIPrefix;
import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.dkm.springles.SpringlesConnection;
import eu.fbk.dkm.springles.SpringlesRepository;
import eu.fbk.dkm.springles.TransactionMode;
import eu.fbk.dkm.springles.base.SynchronizedTransaction.EndListener;

/**
 * Base implementation of <tt>SpringlesRepository</tt>.
 * 
 * @apiviz.landmark
 * @apiviz.uses eu.fbk.dkm.springles.base.SpringlesConnectionBase - - <<create>>
 */
public abstract class SpringlesRepositoryBase implements SpringlesRepository
{

    private enum Status
    {
        NEW, INITIALIZING, INITIALIZED, CLOSING, CLOSED
    };

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringlesRepository.class);

    // Read-only properties (set at construction time by sub-classes)

    private final String id;

    private final URI nullContextURI;

    private final URIPrefix inferredContextPrefix;

    private final Supplier<String> transactionIDSupplier;

    // Configurable properties (frozen after initialize() is called)

    private File dataDir;

    private boolean writable;

    private boolean bufferingEnabled;

    private int maxConcurrentTransactions;

    private long maxTransactionExecutionTime;

    private long maxTransactionIdleTime;

    private ScheduledExecutorService scheduler;

    // Value factory and inference mode available after initialization

    private InferenceMode supportedInferenceMode;

    private ValueFactory valueFactory;

    // Auxiliary member variables

    private boolean schedulerToBeClosed;

    private volatile Status status; // volatile so to avoid reading stale values

    private CountDownLatch statusLatch;

    private Semaphore semaphore;

    private final AtomicLong connectionCounter;

    private final Set<SpringlesConnection> pendingConnections;

    private final Map<String, Transaction> pendingTransactions;

    private final Map<String, TransactionHolder> namedTransactions;

    // CONSTRUCTION

    protected SpringlesRepositoryBase(final String id, final URI nullContextURI,
            final String inferredContextPrefix, final Supplier<String> transactionIDSupplier)
    {
        Preconditions.checkNotNull(id);
        Preconditions.checkNotNull(nullContextURI);
        Preconditions.checkNotNull(inferredContextPrefix);
        Preconditions.checkNotNull(transactionIDSupplier);

        this.id = id;
        this.nullContextURI = nullContextURI;
        this.inferredContextPrefix = URIPrefix.from(inferredContextPrefix);
        this.transactionIDSupplier = transactionIDSupplier;

        this.dataDir = null;
        this.writable = true;
        this.bufferingEnabled = false;
        this.maxConcurrentTransactions = 0; // no limit
        this.maxTransactionExecutionTime = 0L; // no limit
        this.maxTransactionIdleTime = 0L; // no limit
        this.scheduler = null;

        this.schedulerToBeClosed = false;
        this.status = Status.NEW;
        this.statusLatch = null;
        this.semaphore = null;
        this.connectionCounter = new AtomicLong(0);
        this.pendingConnections = Sets.newHashSet();
        this.pendingTransactions = Maps.newHashMap();
        this.namedTransactions = Maps.newHashMap();
    }

    // READ-ONLY PROPERTIES

    @Override
    public String getID()
    {
        return this.id;
    }

    @Override
    public final URI getNullContextURI()
    {
        return this.nullContextURI;
    }

    @Override
    public final String getInferredContextPrefix()
    {
        return this.inferredContextPrefix.toString();
    }

    // CONFIGURABLE PROPERTIES

    @Override
    public final File getDataDir()
    {
        return this.dataDir;
    }

    @Override
    public final void setDataDir(final File dataDir)
    {
        Preconditions.checkState(!isInitialized());
        this.dataDir = dataDir;
    }

    public final boolean isBufferingEnabled()
    {
        return this.bufferingEnabled;
    }

    public final void setBufferingEnabled(final boolean bufferingEnabled)
    {
        Preconditions.checkState(!isInitialized());
        this.bufferingEnabled = bufferingEnabled;
    }

    public final int getMaxConcurrentTransactions()
    {
        return this.maxConcurrentTransactions;
    }

    public final void setMaxConcurrentTransactions(final int maxConcurrentTransactions)
    {
        Preconditions.checkState(!isInitialized());
        this.maxConcurrentTransactions = maxConcurrentTransactions;
    }

    public final long getMaxTransactionExecutionTime()
    {
        return this.maxTransactionExecutionTime;
    }

    public final void setMaxTransactionExecutionTime(final long maxTransactionExecutionTime)
    {
        Preconditions.checkState(!isInitialized());
        this.maxTransactionExecutionTime = maxTransactionExecutionTime;
    }

    public final long getMaxTransactionIdleTime()
    {
        return this.maxTransactionIdleTime;
    }

    public final void setMaxTransactionIdleTime(final long maxTransactionIdleTime)
    {
        Preconditions.checkState(!isInitialized());
        this.maxTransactionIdleTime = maxTransactionIdleTime;
    }

    public final ScheduledExecutorService getScheduler()
    {
        return this.scheduler;
    }

    public final void setScheduler(final ScheduledExecutorService scheduler)
    {
        Preconditions.checkState(!isInitialized());
        this.scheduler = scheduler;
    }

    // STATUS MANAGEMENT: INITIALIZATION AND SHUTDOWN

    @Override
    public final boolean isInitialized()
    {
        return this.status == Status.INITIALIZED;
    }

    @Override
    public final synchronized void initialize() throws RepositoryException
    {
        boolean executeInitialize = true; // if false, wait for initialization to complete

        synchronized (this) {
            switch (this.status) {
            case INITIALIZED:
                return;
            case CLOSING:
            case CLOSED:
                throw new IllegalStateException(
                        "Cannot initialize: repository is closed / closing");
            case INITIALIZING:
                executeInitialize = false;
                break;
            case NEW:
                this.statusLatch = new CountDownLatch(1);
                this.status = Status.INITIALIZING;
                break;
            default:
                throw new Error();
            }
        }

        if (executeInitialize) {
            // it is guaranteed a single thread can enter here
            boolean success = false;
            try {
                this.semaphore = new Semaphore(
                        this.maxConcurrentTransactions > 0 ? this.maxConcurrentTransactions
                                : Short.MAX_VALUE, false);
                if (this.scheduler == null && this.maxTransactionExecutionTime > 0
                        && this.maxTransactionIdleTime > 0) {
                    final ThreadFactory factory = new ThreadFactoryBuilder().setDaemon(true)
                            .setNameFormat("springles-monitor-{}").build();
                    this.scheduler = Executors.newScheduledThreadPool(1, factory);
                    this.schedulerToBeClosed = true;
                }

                final AtomicReference<ValueFactory> factory = new AtomicReference<ValueFactory>();
                final AtomicReference<InferenceMode> mode = new AtomicReference<InferenceMode>();
                // this.valueFactory = doInitialize();
                this.writable = doInitialize(factory, mode);
                this.valueFactory = factory.get();
                this.supportedInferenceMode = mode.get();

                Preconditions.checkNotNull(this.valueFactory);
                Preconditions.checkNotNull(this.supportedInferenceMode);

                success = true;
                LOGGER.info("[{}] Repository initialized, data dir: {}", this.id, this.dataDir);

            } finally {
                synchronized (this) {
                    this.status = success ? Status.INITIALIZED : Status.CLOSED;
                    this.statusLatch.countDown();
                }
            }

        } else {
            try {
                this.statusLatch.await();
                if (this.status == Status.CLOSED) {
                    throw new RepositoryException("Initialization failed");
                }
            } catch (final InterruptedException ex) {
                throw new RepositoryException("Thread interrupted while waiting "
                        + "for repository initialization to complete", ex);
            }
        }
    }

    @Override
    public final void shutDown() throws RepositoryException
    {
        boolean executeShutdown = false;
        boolean waitInitializationCompletion = false;

        synchronized (this) {
            switch (this.status) {
            case NEW:
                this.status = Status.CLOSED;
            case CLOSED:
                return;
            case INITIALIZING:
                waitInitializationCompletion = true;
            case CLOSING:
                break;
            case INITIALIZED:
                executeShutdown = true;
                this.statusLatch = new CountDownLatch(1);
                this.status = Status.CLOSING;
                break;
            default:
                throw new Error();
            }
        }

        if (waitInitializationCompletion) {
            try {
                this.statusLatch.await();
            } catch (final InterruptedException ex) {
                throw new RepositoryException("Thread interrupted while waiting "
                        + "for repository initialization to complete", ex);
            }
            if (this.status != Status.CLOSED) {
                shutDown();
            }
            return;
        }

        if (executeShutdown) {
            // it is guaranteed a single thread can enter here
            closePendingConnections();
            rollbackPendingTransactions();
            try {
                doShutdown();
            } catch (final Throwable ex) {
                LOGGER.error("[" + this.id + "] Got an exception performing shutdown. Ignoring",
                        ex);
            }
            if (this.schedulerToBeClosed) {
                this.scheduler.shutdown();
            }
            LOGGER.info("[" + this.id + "] Repository shutted down");
            synchronized (this) {
                this.status = Status.CLOSED;
                this.statusLatch.countDown();
            }

        } else {
            try {
                this.statusLatch.await();
            } catch (final InterruptedException ex) {
                throw new RepositoryException("Thread interrupted while waiting "
                        + "for repository shutdown to complete", ex);
            }
        }
    }

    /**
     * Hook for performing subclass specific initialization logic. This method is called as part
     * of the execution of {@link #initialize()} and is synchronized externally. Exceptions thrown
     * by this method will cause the initialization of the repository to fail.
     * 
     * @param valueFactoryHolder
     *            a reference to the repository <tt>ValueFactory</tt> to be set by the method
     * @param inferenceModeHolder
     *            a reference to the repository <tt>InferenceMode</tt> to be set by the method
     * @return <tt>true</tt> if writing access is supported
     * @throws RepositoryException
     *             in initialization failed for reasons different from configuration errors.
     */
    protected abstract boolean doInitialize(AtomicReference<ValueFactory> valueFactoryHolder,
            AtomicReference<InferenceMode> inferenceModeHolder) throws RepositoryException;

    /**
     * Hook for performing subclass specific shutdown logic. This method is called as part of the
     * execution of {@link #shutDown()} and is synchronized externally. This method should not
     * throw exceptions, but simply log errors; if any exception is thrown, this will be caught
     * externally, logged and ignored, so the whole shutdown process can complete.
     */
    protected void doShutdown()
    {
        // may be overridden by sub-classes
    }

    // VALUE FACTORY ACCESS

    @Override
    public final boolean isWritable()
    {
        Preconditions.checkState(this.status == Status.INITIALIZED);
        return this.writable;
    }

    @Override
    public final InferenceMode getInferenceMode()
    {
        Preconditions.checkState(this.status == Status.INITIALIZED);
        return this.supportedInferenceMode;
    }

    @Override
    public final ValueFactory getValueFactory()
    {
        Preconditions.checkState(this.status == Status.INITIALIZED);
        return this.valueFactory;
    }

    // CONNECTION HANDLING

    @Override
    public final synchronized SpringlesConnection getConnection() throws RepositoryException
    {
        Preconditions.checkState(this.status == Status.INITIALIZED);

        final String id = this.id + ":con" + this.connectionCounter.incrementAndGet();

        final SpringlesConnection connection = createConnection(id);
        this.pendingConnections.add(connection);

        LOGGER.debug("[{}] Connection created, transaction mode: {}", id,
                connection.getTransactionMode());

        return connection;
    }

    /**
     * Hook for the creation of {@link SpringlesConnection}s. The default implementation creates a
     * {@link SpringlesConnectionBase}. Subclasses may override this method to return specific
     * subclasses of {@link SpringlesConnectionBase}.
     * 
     * @param id
     *            the ID of the connection to create
     * @return a new, possibly subclass-specific, repository connection
     * @throws RepositoryException
     *             on failure
     */
    protected SpringlesConnectionBase createConnection(final String id) throws RepositoryException
    {
        return new SpringlesConnectionBase(id, this);
    }

    synchronized void onConnectionClosed(final SpringlesConnectionBase connection)
    {
        this.pendingConnections.remove(connection);
        LOGGER.debug("[{}] Connection closed", connection.getID());
    }

    private void closePendingConnections()
    {
        List<SpringlesConnection> connectionsToClose;
        synchronized (this) {
            connectionsToClose = Lists.newArrayList(this.pendingConnections);
        }

        if (connectionsToClose.isEmpty()) {
            return;
        }

        LOGGER.debug("[{}] Forcing closure of {} pending connections", this.id,
                connectionsToClose.size());

        for (final SpringlesConnection connection : connectionsToClose) {
            try {
                connection.close();
            } catch (final Throwable ex) {
                LOGGER.error("[" + connection.getID()
                        + "] Got exception while closing connection. Ignoring", ex);
            }
        }
    }

    // TRANSACTION HANDLING

    final Transaction getTransaction(final TransactionMode mode, final boolean autoCommit,
            final String name) throws RepositoryException
    {
        Preconditions.checkNotNull(mode);
        Preconditions.checkNotNull(name);

        synchronized (this) {
            Preconditions.checkState(isInitialized());
            final TransactionHolder holder = this.namedTransactions.get(name);
            if (holder != null) {
                if (autoCommit != holder.isAutoCommit() || mode != holder.getTransactionMode()) {
                    throw new RepositoryException("Supplied transaction mode and/or auto-commit "
                            + "flag do not match existing named transaction '" + name + "'");
                }
                return holder.getTransaction();
            }
        }

        final Transaction transaction = getTransaction(mode, autoCommit, new EndListener() {

            @Override
            public void transactionEnded(final boolean committed,
                    final ClosureStatus newClosureStatus)
            {
                SpringlesRepositoryBase.this.namedTransactions.remove(name);
            }

        });

        synchronized (this) {
            Preconditions.checkState(isInitialized()); // repository may have been closed
            if (!autoCommit) {
                this.namedTransactions.put(name, new TransactionHolder(transaction, mode,
                        autoCommit));
                LOGGER.debug("[{}] Transaction bound to name '{}'", transaction.getID(), name);
            }
            return transaction;
        }
    }

    final Transaction getTransaction(final TransactionMode mode, final boolean autoCommit,
            final EndListener listener) throws RepositoryException
    {
        Preconditions.checkState(isInitialized());
        Preconditions.checkNotNull(mode);

        final TransactionMode actualMode = selectTransactionMode(mode);
        final int numPermits = actualMode == TransactionMode.READ_ONLY ? 1
                : this.maxConcurrentTransactions;

        try {
            this.semaphore.acquire(numPermits);
        } catch (final InterruptedException ex) {
            throw new RepositoryException(
                    "Thread interrupted while waiting authorization to start transaction", ex);
        }

        final String transactionID = this.transactionIDSupplier.get();
        Transaction transaction = null;
        try {
            synchronized (this) {
                Preconditions.checkState(isInitialized());

                transaction = createTransactionStack(transactionID, autoCommit, actualMode,
                        new EndListener() {

                            @Override
                            public void transactionEnded(final boolean committed,
                                    final ClosureStatus newClosureStatus)
                            {
                                SpringlesRepositoryBase.this.semaphore.release(numPermits);

                                synchronized (SpringlesRepositoryBase.this) {
                                    SpringlesRepositoryBase.this.pendingTransactions
                                            .remove(transactionID);
                                }

                                if (listener != null) {
                                    listener.transactionEnded(committed, newClosureStatus);
                                }
                            }

                        });

                if (this.bufferingEnabled && !autoCommit
                        && actualMode != TransactionMode.READ_ONLY) {
                    transaction = new BufferingTransaction(transaction);
                }

                this.pendingTransactions.put(transactionID, transaction);
            }
        } finally {
            if (transaction == null) {
                this.semaphore.release(numPermits);
            }
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[" + transactionID + "] Transaction created, auto-commit: " + autoCommit
                    + ", mode: " + actualMode);
        }

        return transaction;
    }

    private TransactionMode selectTransactionMode(final TransactionMode requestedMode)
    {
        if (!this.writable && requestedMode != TransactionMode.READ_ONLY) {
            throw new IllegalStateException(
                    "Cannot start a write transaction on read-only repository");
        }

        if (!this.supportedInferenceMode.isForwardEnabled()
                && requestedMode == TransactionMode.WRITABLE_AUTO_CLOSURE) {
            return TransactionMode.WRITABLE_MANUAL_CLOSURE;
        } else {
            return requestedMode;
        }
    }

    private Transaction createTransactionStack(final String transactionID,
            final boolean autoCommit, final TransactionMode transactionMode,
            final EndListener listener) throws RepositoryException
    {
        Transaction transaction = createTransactionRoot(transactionID, transactionMode, autoCommit);

        transaction = new ContextEnforcingTransaction(transaction, getNullContextURI(),
                this.inferredContextPrefix);

        transaction = decorateTransactionInternally(transaction, transactionMode, autoCommit);

        final boolean writable = transactionMode != TransactionMode.READ_ONLY;
        final boolean autoClosure = transactionMode == TransactionMode.WRITABLE_AUTO_CLOSURE;
        if (this.scheduler != null) {
            transaction = new SynchronizedTransaction(transaction, writable, autoCommit,
                    autoClosure, listener, this.maxTransactionExecutionTime,
                    this.maxTransactionIdleTime, this.scheduler);
        } else {
            transaction = new SynchronizedTransaction(transaction, writable, autoCommit,
                    autoClosure, listener);
        }

        transaction = decorateTransactionExternally(transaction, transactionMode, autoCommit);

        if (this.bufferingEnabled && !autoCommit && transactionMode != TransactionMode.READ_ONLY) {
            transaction = new BufferingTransaction(transaction);
        }

        return transaction;
    }

    /**
     * Hook for the creation of root (un-decorated) transactions. This must be implemented by
     * subclasses and is called each time a new transaction is created. It is a responsibility of
     * the returned transaction to properly handle inference.
     * 
     * @param transactionID
     *            a string identifier for the new transaction
     * @param transactionMode
     *            the transaction mode, guaranteed to be compatible with the inference and writing
     *            capabilities of the repository.
     * @param autoCommit
     *            <tt>true</tt> if the transaction will be used in auto-commit mode. Note that
     *            auto-committing is handled in {@link SpringlesRepositoryBase}; this parameter is
     *            supplied only because it can provide some opportunity for optimizing the created
     *            transaction
     * @return the created transaction
     * @throws RepositoryException
     *             on failure
     */
    protected abstract Transaction createTransactionRoot(final String transactionID,
            final TransactionMode transactionMode, final boolean autoCommit)
            throws RepositoryException;

    /**
     * Hook for decorating a transaction internally, before it is enhanced with synchronization,
     * auto-commit and auto-closure support. This method is supplied with the transaction returned
     * by {@link #createTransactionRoot(String, TransactionMode, boolean)}, protected with
     * context-filtering so to exclude modifications of inferred statements. The default
     * implementation of this method returns the supplied transaction unchanged. Subclasses may
     * override the method decorating the transaction as they wish.
     * 
     * @param transaction
     *            the transaction to decorate
     * @param transactionMode
     *            the transaction mode, guaranteed to be compatible with the inference and writing
     *            capabilities of the repository
     * @param autoCommit
     *            <tt>true</tt> if the transaction will be used in auto-commit mode; this
     *            parameter is only informative, as auto-committing support will be added
     *            externally
     * @return the decorated transaction
     * @throws RepositoryException
     *             on failure
     */
    protected Transaction decorateTransactionInternally(final Transaction transaction,
            final TransactionMode transactionMode, final boolean autoCommit)
            throws RepositoryException
    {
        return transaction; // may be overridden
    }

    /**
     * Hook for decorating a transaction before it is supplied for use by a
     * {@link SpringlesConnectionBase} (after the optional addition of buffering support). This
     * method is supplied with the transaction resulting from
     * {@link #createTransactionRoot(String, TransactionMode, boolean)}, protected with context
     * filtering, decorated interally as resulting from
     * {@link #decorateTransactionInternally(Transaction, TransactionMode, boolean)} and enhanced
     * with synchronization, auto-commit and auto-closure support. The default implementation of
     * this method returns the supplied transaction unchanged. Subclasses may override the method
     * decorating the transaction as they wish.
     * 
     * @param transaction
     *            the transaction to decorate
     * @param transactionMode
     *            the transaction mode, guaranteed to be compatible with the inference and writing
     *            capabilities of the repository
     * @param autoCommit
     *            <tt>true</tt> if the transaction will be used in auto-commit mode; this
     *            parameter is only informative, as auto-committing is already implemented in the
     *            supplied transaction
     * @return the decorated transaction
     * @throws RepositoryException
     *             on failure
     */
    protected Transaction decorateTransactionExternally(final Transaction transaction,
            final TransactionMode transactionMode, final boolean autoCommit)
            throws RepositoryException
    {
        return transaction; // may be overridden
    }

    private void rollbackPendingTransactions()
    {
        List<Transaction> transactionsToRollback;
        synchronized (this) {
            transactionsToRollback = Lists.newArrayList(this.pendingTransactions.values());
        }

        if (transactionsToRollback.isEmpty()) {
            return;
        }

        LOGGER.debug("[{}] Forcing rollback of {} pending transactions", this.id,
                transactionsToRollback.size());

        for (final Transaction transaction : transactionsToRollback) {
            try {
                transaction.end(false);
            } catch (final Throwable ex) {
                LOGGER.error("[" + transaction.getID()
                        + "] Got exception while closing transaction. Ignoring", ex);
            }
        }
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this).add("nullContextURI", this.nullContextURI)
                .add("inferredContextPrefix", this.inferredContextPrefix)
                .add("supportedInferenceMode", this.supportedInferenceMode)
                .add("writable", this.writable).add("bufferingEnabled", this.bufferingEnabled)
                .add("maxConcurrentTransactions", this.maxConcurrentTransactions)
                .add("maxTransactionExecutionTime", this.maxTransactionExecutionTime)
                .add("maxTransactionIdleTime", this.maxTransactionIdleTime).toString();
    }

    private static class TransactionHolder
    {

        private final Transaction transaction;

        private final TransactionMode transactionMode;

        private final boolean autoCommit;

        public TransactionHolder(final Transaction transaction,
                final TransactionMode transactionMode, final boolean autoCommit)
        {
            this.transaction = transaction;
            this.transactionMode = transactionMode;
            this.autoCommit = autoCommit;
        }

        public Transaction getTransaction()
        {
            return this.transaction;
        }

        public TransactionMode getTransactionMode()
        {
            return this.transactionMode;
        }

        public boolean isAutoCommit()
        {
            return this.autoCommit;
        }

    }

}
