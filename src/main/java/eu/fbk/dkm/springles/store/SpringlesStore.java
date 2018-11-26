package eu.fbk.dkm.springles.store;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import eu.fbk.dkm.internal.springles.protocol.Settings;
import eu.fbk.dkm.internal.util.Selector;
import eu.fbk.dkm.internal.util.URIPrefix;
import eu.fbk.dkm.springles.Factory;
import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.dkm.springles.SPC;
import eu.fbk.dkm.springles.TransactionMode;
import eu.fbk.dkm.springles.backend.Backend;
import eu.fbk.dkm.springles.backend.Backends;
import eu.fbk.dkm.springles.base.SpringlesRepositoryBase;
import eu.fbk.dkm.springles.base.Transaction;
import eu.fbk.dkm.springles.inferencer.Inferencer;
import eu.fbk.dkm.springles.inferencer.Inferencers;

/**
 * Store implementation.
 *
 * @apiviz.landmark
 * @apiviz.owns eu.fbk.dkm.springles.backend.Backend
 * @apiviz.owns eu.fbk.dkm.springles.inference.Inferencer
 * @apiviz.owns eu.fbk.dkm.springles.store.Interceptor - *
 */
public class SpringlesStore extends SpringlesRepositoryBase
{

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringlesStore.class);

    private final Backend backend;

    private final Inferencer inferencer;

    private boolean serverExtensionEnabled;

    // XXX cannot reuse URIPrefix instance in parent class, as the decision is not to expose
    // URIPrefix at API level (this may change should it be refactored in a util library).
    private final URIPrefix inferredContextPrefix;

    // CONSTRUCTION

    public SpringlesStore(final String id, final Backend backend, final Inferencer inferencer,
            final IRI nullContextURI, final String inferredContextURIPrefix)
    {
        super(id, nullContextURI, //
                inferredContextURIPrefix, //
                new Supplier<String>() {

                    private final AtomicLong counter = new AtomicLong(0);

                    @Override
                    public String get()
                    {
                        return id + ":tx" + this.counter.incrementAndGet();
                    }

                });

        final Logger backendLogger = LoggerFactory.getLogger(Backend.class);
        final Logger inferencerLogger = LoggerFactory.getLogger(Inferencer.class);

        this.backend = !backendLogger.isInfoEnabled() ? backend : //
                Backends.debuggingBackend(backend, backendLogger);
        this.inferencer = !inferencerLogger.isInfoEnabled() ? inferencer : //
                Inferencers.debuggingInferencer(inferencer, inferencerLogger);
        this.serverExtensionEnabled = true;
        this.inferredContextPrefix = URIPrefix.from(inferredContextURIPrefix);
    }

    // CONFIGURABLE PROPERTIES

    public final boolean isServerExtensionEnabled()
    {
        return this.serverExtensionEnabled;
    }

    public final void setServerExtensionEnabled(final boolean serverExtensionEnabled)
    {
        Preconditions.checkState(!this.isInitialized());
        this.serverExtensionEnabled = serverExtensionEnabled;
    }

    // INITIALIZATION AND SHUTDOWN

    @Override
    protected boolean doInitialize(final AtomicReference<ValueFactory> valueFactoryHolder,
            final AtomicReference<InferenceMode> inferenceModeHolder) throws RepositoryException
    {
        int counter = 0; // number of initialized objects, -1 on success

        try {
            this.backend.initialize(this.getDataDir());
            counter++;

            this.inferencer.initialize(this.inferredContextPrefix.getPrefix());
            counter++;

            counter = -1;

            valueFactoryHolder.set(this.backend.getValueFactory());
            inferenceModeHolder.set(this.inferencer.getInferenceMode());

            return this.backend.isWritable();

        } finally {
            if (counter >= 0) {
                SpringlesStore.LOGGER.error(
                        "[{}] Initialization failed. Close initialized resources.", this.getID());
            }

            if (counter >= 2) {
                this.closeQuietly(this.inferencer);
            }

            if (counter >= 1) {
                this.closeQuietly(this.backend);
            }
        }
    }

    @Override
    protected void doShutdown()
    {
        this.closeQuietly(this.inferencer);
        this.closeQuietly(this.backend);
    }

    private void closeQuietly(final Backend backend)
    {
        try {
            backend.close();
        } catch (final Throwable ex) {
            SpringlesStore.LOGGER
                    .error("[" + this.getID() + "] Got exception while closing backend "
                            + backend.getClass().getSimpleName() + ". Ignoring.", ex);
        }
    }

    private void closeQuietly(final Inferencer inferencer)
    {
        try {
            inferencer.close();
        } catch (final Throwable ex) {
            SpringlesStore.LOGGER
                    .error("[" + this.getID() + "] Got exception while closing inference engine "
                            + inferencer.getClass().getSimpleName() + ". Ignoring.", ex);
        }
    }

    // TRANSACTION CREATION

    @Override
    protected Transaction createTransactionRoot(final String transactionID,
            final TransactionMode transactionMode, final boolean autoCommit)
            throws RepositoryException
    {
        Transaction transaction = this.backend.newTransaction(transactionID,
                transactionMode != TransactionMode.READ_ONLY);

        final File dataDir = this.getDataDir();
        final File closureMetadataFile = dataDir == null ? null
                : new File(this.getDataDir(), "closure.status");

        transaction = new InferenceTransaction(transaction, this.inferredContextPrefix,
                this.inferencer, this.getScheduler(), closureMetadataFile, this);

        return transaction;
    }

    @Override
    protected Transaction decorateTransactionInternally(final Transaction transaction,
            final TransactionMode transactionMode, final boolean autoCommit)
            throws RepositoryException
    {
        final Transaction decoratedTransaction = super.decorateTransactionInternally(transaction,
                transactionMode, autoCommit);

        return decoratedTransaction;
    }

    @Override
    protected Transaction decorateTransactionExternally(final Transaction transaction,
            final TransactionMode transactionMode, final boolean autoCommit)
            throws RepositoryException
    {
        final Transaction decoratedTransaction = super.decorateTransactionExternally(transaction,
                transactionMode, autoCommit);

        return decoratedTransaction;
    }

    @Override
    public String toString()
    {
        String parent = super.toString();
        parent = parent.substring(parent.indexOf('{') + 1, parent.lastIndexOf('}'));
        return MoreObjects.toStringHelper(this).addValue(parent).add("backend", this.backend)
                .add("inferencer", this.inferencer)
                .add("serverExtensionEnabled", this.serverExtensionEnabled).toString();
    }

    static Factory<SpringlesStore> getFactory(final Model graph, final Resource node)
            throws RepositoryConfigException
    {
        try {
            final Selector s = Selector.select(graph, node);

            final String id = s.get(SPC.HAS_ID, String.class);

            final IRI nullContextURI = s.get(SPC.HAS_NULL_CONTEXT_URI, IRI.class);
            final String inferredContextPrefix = s.get(SPC.HAS_INFERRED_CONTEXT_PREFIX,
                    String.class);

            final boolean serverExtensionEnabled = s.isSet(SPC.HAS_SERVER_EXTENSION_ENABLED);
            final boolean bufferingEnabled = s.isSet(SPC.HAS_BUFFERING_ENABLED);

            final int maxConcurrentTransactions = s.get(SPC.HAS_MAX_CONCURRENT_TRANSACTIONS, 0);
            final long maxTransactionIdleTime = s.get(SPC.HAS_MAX_TRANSACTION_IDLE_TIME, 0L);
            final long maxTransactionExecutionTime = s.get(SPC.HAS_MAX_TRANSACTION_EXECUTION_TIME,
                    0L);

            final Factory<Backend> backendFactory = Factory.get(Backend.class, graph,
                    s.get(SPC.HAS_BACKEND, Resource.class));

            final Factory<Inferencer> inferencerFactory = Factory.get(Inferencer.class, graph,
                    s.get(SPC.HAS_INFERENCER, Resource.class));

            return new Factory<SpringlesStore>() {

                @Override
                public SpringlesStore create() throws RepositoryException
                {
                    final SpringlesStore store = new SpringlesStore(id, backendFactory.create(),
                            inferencerFactory.create(), nullContextURI, inferredContextPrefix);
                    store.setServerExtensionEnabled(serverExtensionEnabled);
                    store.setBufferingEnabled(bufferingEnabled);
                    store.setMaxConcurrentTransactions(maxConcurrentTransactions);
                    store.setMaxTransactionIdleTime(maxTransactionIdleTime);
                    store.setMaxTransactionExecutionTime(maxTransactionExecutionTime);
                    if (SpringlesStore.LOGGER.isInfoEnabled()) {
                        SpringlesStore.LOGGER.info("[" + id + "] Repository created:\n"
                                + store.toString().replace(",", ",\n  "));
                    }
                    return store;
                }

            };

        } catch (final RuntimeException ex) {
            throw new RepositoryConfigException("Invalid configuration: " + ex.getMessage(), ex);
        }
    }

}
