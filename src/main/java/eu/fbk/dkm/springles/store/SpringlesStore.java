package eu.fbk.dkm.springles.store;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.config.RepositoryConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

//import eu.fbk.dkm.internal.springles.protocol.Settings;
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

    private static final int BASE = 32;

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringlesStore.class);

    private final Backend backend;

    private final Inferencer inferencer;

    private boolean serverExtensionEnabled;

  //  private List<Interceptor> preInferenceInterceptors;

  //  private List<Interceptor> postInferenceInterceptors;

    // XXX cannot reuse URIPrefix instance in parent class, as the decision is not to expose
    // URIPrefix at API level (this may change should it be refactored in a util library).
    private final URIPrefix inferredContextPrefix;

 //   private Supplier<Settings> settingsSupplier;

    // CONSTRUCTION

    public SpringlesStore(final String id, final Backend backend, final Inferencer inferencer,
            final URI nullContextURI, final String inferredContextURIPrefix)
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

        this.backend = !backendLogger.isDebugEnabled() ? backend : //
                Backends.debuggingBackend(backend, backendLogger);
        this.inferencer = !inferencerLogger.isDebugEnabled() ? inferencer : //
                Inferencers.debuggingInferencer(inferencer, inferencerLogger);

        this.serverExtensionEnabled = true;
   //     this.preInferenceInterceptors = Collections.emptyList();
    //    this.postInferenceInterceptors = Collections.emptyList();

        this.inferredContextPrefix = URIPrefix.from(inferredContextURIPrefix);

   //     this.settingsSupplier = new Supplier<Settings>() {

             final long lastID = 0;

            /*          @Override
            public synchronized Settings get()
            {
                this.lastID = Math.max(System.currentTimeMillis(), this.lastID + 1);
                return new Settings(Long.toString(this.lastID, BASE), getNullContextURI(),
                        getInferredContextPrefix(), getInferenceMode(), isWritable());
            }

        };*/
    }

    // CONFIGURABLE PROPERTIES

    public final boolean isServerExtensionEnabled()
    {
        return this.serverExtensionEnabled;
    }

    public final void setServerExtensionEnabled(final boolean serverExtensionEnabled)
    {
        Preconditions.checkState(!isInitialized());
        this.serverExtensionEnabled = serverExtensionEnabled;
    }

/*    public final List<Interceptor> getPreInferenceInterceptors()
    {
        return this.preInferenceInterceptors;
    }*/

 /*   public final void setPreInferenceInterceptors(
            final Iterable<Interceptor> preInferenceInterceptors)
    {
        Preconditions.checkState(!isInitialized());
        this.preInferenceInterceptors = preInferenceInterceptors == null ? Collections
                .<Interceptor>emptyList() : ImmutableList.copyOf(preInferenceInterceptors);
    }*/

 /*   public final List<Interceptor> getPostInferenceInterceptors()
    {
        return this.postInferenceInterceptors;
    }*/

  /*  public final void setPostInferenceInterceptors(
            final Iterable<Interceptor> postInferenceInterceptors)
    {
        Preconditions.checkState(!isInitialized());
        this.postInferenceInterceptors = postInferenceInterceptors == null ? Collections
                .<Interceptor>emptyList() : ImmutableList.copyOf(postInferenceInterceptors);
    }*/

    // INITIALIZATION AND SHUTDOWN

    @Override
    protected boolean doInitialize(final AtomicReference<ValueFactory> valueFactoryHolder,
            final AtomicReference<InferenceMode> inferenceModeHolder) throws RepositoryException
    {
        int counter = 0; // number of initialized objects, -1 on success

    /*    final Iterable<Interceptor> interceptors = Iterables.concat(this.preInferenceInterceptors,
                this.postInferenceInterceptors);*/

        try {
            this.backend.initialize(getDataDir());
            counter++;

            this.inferencer.initialize(this.inferredContextPrefix.getPrefix());
            counter++;

       /*     for (final Interceptor interceptor : interceptors) {
                interceptor.initialize();
                counter++;
            }*/

            counter = -1;

            valueFactoryHolder.set(this.backend.getValueFactory());
            inferenceModeHolder.set(this.inferencer.getInferenceMode());

            return this.backend.isWritable();

        } finally {
            if (counter >= 0) {
                LOGGER.error("[{}] Initialization failed. Close initialized resources.", getID());
            }

   //         closeQuietly(Iterables.limit(interceptors, Math.max(0, counter - 2)));

            if (counter >= 2) {
                closeQuietly(this.inferencer);
            }

            if (counter >= 1) {
                closeQuietly(this.backend);
            }
        }
    }

    @Override
    protected void doShutdown()
    {
  //      closeQuietly(this.postInferenceInterceptors);
  //      closeQuietly(this.preInferenceInterceptors);
        closeQuietly(this.inferencer);
        closeQuietly(this.backend);
    }

    private void closeQuietly(final Backend backend)
    {
        try {
            backend.close();
        } catch (final Throwable ex) {
            LOGGER.error("[" + getID() + "] Got exception while closing backend "
                    + backend.getClass().getSimpleName() + ". Ignoring.", ex);
        }
    }

    private void closeQuietly(final Inferencer inferencer)
    {
        try {
            inferencer.close();
        } catch (final Throwable ex) {
            LOGGER.error("[" + getID() + "] Got exception while closing inference engine "
                    + inferencer.getClass().getSimpleName() + ". Ignoring.", ex);
        }
    }

 /*   private void closeQuietly(final Iterable<Interceptor> interceptors)
    {
        for (final Interceptor interceptor : interceptors) {
            try {
                interceptor.close();
            } catch (final Throwable ex) {
                LOGGER.error("[" + getID() + "] Got exception while closing interceptor "
                        + interceptor.getClass().getSimpleName() + ". Ignoring.", ex);
            }
        }
    }*/

    // TRANSACTION CREATION

    @Override
    protected Transaction createTransactionRoot(final String transactionID,
            final TransactionMode transactionMode, final boolean autoCommit)
            throws RepositoryException
    {
        Transaction transaction = this.backend.newTransaction(transactionID,
                transactionMode != TransactionMode.READ_ONLY);

 /*       for (int i = this.postInferenceInterceptors.size() - 1; i >= 0; --i) {
            transaction = this.postInferenceInterceptors.get(i).intercept(transaction,
                    transactionMode);
            Preconditions.checkNotNull(transaction);
        }*/

        final File dataDir = getDataDir();
        final File closureMetadataFile = dataDir == null ? null : new File(getDataDir(),
                "closure.status");

        transaction = new InferenceTransaction(transaction,
                this.inferredContextPrefix,inferencer, getScheduler(), closureMetadataFile,this);

        return transaction;
    }

    @Override
    protected Transaction decorateTransactionInternally(final Transaction transaction,
            final TransactionMode transactionMode, final boolean autoCommit)
            throws RepositoryException
    {
        Transaction decoratedTransaction = super.decorateTransactionInternally(transaction,
                transactionMode, autoCommit);

   /*     for (int i = this.preInferenceInterceptors.size() - 1; i >= 0; --i) {
            decoratedTransaction = this.preInferenceInterceptors.get(i).intercept(
                    decoratedTransaction, transactionMode);
            Preconditions.checkNotNull(decoratedTransaction);
        }*/

        return decoratedTransaction;
    }

    @Override
    protected Transaction decorateTransactionExternally(final Transaction transaction,
            final TransactionMode transactionMode, final boolean autoCommit)
            throws RepositoryException
    {
        Transaction decoratedTransaction = super.decorateTransactionExternally(transaction,
                transactionMode, autoCommit);

   /*     if (this.serverExtensionEnabled) {
            decoratedTransaction = new ServerTransaction(decoratedTransaction,
                    this.settingsSupplier);
        }*/

        return decoratedTransaction;
    }

    @Override
    public String toString()
    {
        String parent = super.toString();
        parent = parent.substring(parent.indexOf('{') + 1, parent.lastIndexOf('}'));
        return Objects.toStringHelper(this).addValue(parent).add("backend", this.backend)
                .add("inferencer", this.inferencer)
                .add("serverExtensionEnabled", this.serverExtensionEnabled).toString();
       //         .add("preInferenceInterceptors", this.preInferenceInterceptors)
       //         .add("postInferenceInterceptors", this.postInferenceInterceptors).toString();
    }

    static Factory<SpringlesStore> getFactory(final Graph graph, final Resource node)
            throws RepositoryConfigException
    {
        try {
            final Selector s = Selector.select(graph, node);

            final String id = s.get(SPC.HAS_ID, String.class);

            final URI nullContextURI = s.get(SPC.HAS_NULL_CONTEXT_URI, URI.class);
            final String inferredContextPrefix = s.get(SPC.HAS_INFERRED_CONTEXT_PREFIX,
                    String.class);

            final boolean serverExtensionEnabled = s.isSet(SPC.HAS_SERVER_EXTENSION_ENABLED);
            final boolean bufferingEnabled = s.isSet(SPC.HAS_BUFFERING_ENABLED);

            final int maxConcurrentTransactions = s.get(SPC.HAS_MAX_CONCURRENT_TRANSACTIONS, 0);
            final long maxTransactionIdleTime = s.get(SPC.HAS_MAX_TRANSACTION_IDLE_TIME, 0L);
            final long maxTransactionExecutionTime = s.get(
                    SPC.HAS_MAX_TRANSACTION_EXECUTION_TIME, 0L);

            final Factory<Backend> backendFactory = Factory.get(Backend.class, graph,
                    s.get(SPC.HAS_BACKEND, Resource.class));

            final Factory<Inferencer> inferencerFactory = Factory.get(Inferencer.class, graph,
                    s.get(SPC.HAS_INFERENCER, Resource.class));
                       

 /*           final Factory<List<Interceptor>> preInfFactory = Factory.get(Interceptor.class, graph,
                    s.getList(SPC.HAS_PRE_INFERENCE_INTERCEPTORS, Resource.class));
*/
/*            final Factory<List<Interceptor>> postInfFactory = Factory.get(Interceptor.class,
                    graph, s.getList(SPC.HAS_POST_INFERENCE_INTERCEPTORS, Resource.class));
*/
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
      //              store.setPreInferenceInterceptors(preInfFactory.create());
      //              store.setPostInferenceInterceptors(postInfFactory.create());
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("[" + id + "] Repository created:\n"
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
