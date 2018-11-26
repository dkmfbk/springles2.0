package eu.fbk.dkm.springles.base;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.Iteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.query.algebra.Clear;
import org.eclipse.rdf4j.query.impl.AbstractQuery;
import org.eclipse.rdf4j.query.impl.AbstractUpdate;
import org.eclipse.rdf4j.query.impl.IteratingTupleQueryResult;
import org.eclipse.rdf4j.query.impl.ListBindingSet;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.UnknownTransactionStateException;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import eu.fbk.dkm.internal.springles.protocol.Options;
import eu.fbk.dkm.internal.util.Iterations;
import eu.fbk.dkm.internal.util.RDFParseOptions;
import eu.fbk.dkm.internal.util.RDFSource;
import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.dkm.springles.SpringlesConnection;
import eu.fbk.dkm.springles.SpringlesRepository;
import eu.fbk.dkm.springles.TransactionMode;
import eu.fbk.dkm.springles.base.SynchronizedTransaction.EndListener;
import eu.fbk.dkm.springles.base.Transaction.Operation;
import eu.fbk.dkm.springles.ruleset.Ruleset;
import eu.fbk.dkm.springles.ruleset.Rulesets;
import eu.fbk.dkm.springles.ruleset.RulesetsRDFPRO;

/**
 * Base implementation of <tt>SpringlesConnection</tt>.
 *
 * @apiviz.landmark
 * @apiviz.uses eu.fbk.dkm.springles.base.Transaction - - <<delegate>>
 */
public class SpringlesConnectionBase implements SpringlesConnection
{

    private enum Status
    {
        IDLE, READING, WRITING, CLOSING, CLOSED
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringlesConnectionBase.class);

    private static final ParserConfig DEFAULT_PARSER_CONFIG = new ParserConfig()
            .set(BasicParserSettings.VERIFY_RELATIVE_URIS, true)
            .set(BasicParserSettings.NORMALIZE_DATATYPE_VALUES, true)
            .set(BasicParserSettings.PRESERVE_BNODE_IDS, false);

    private static final int BATCH_SIZE = 64 * 1024;

    // ID and repository object (called to obtain value factory, inf. mode and transactions)

    private final String id;

    private final SpringlesRepositoryBase repository;

    // Status variables controlled by users.

    private volatile ParserConfig parserConfig;

    private volatile InferenceMode inferenceMode;

    // Status management

    private volatile Status status;

    private final Map<String, Transaction> pendingTransactions;

    private CountDownLatch closeLatch;

    // Transaction handling

    private Transaction lastTransaction;

    private TransactionMode lastTransactionMode;

    private volatile TransactionMode currentTransactionMode;

    private boolean autoCommit;

    protected SpringlesConnectionBase(final String id, final SpringlesRepositoryBase repository)
            throws RepositoryException
    {
        Preconditions.checkNotNull(id);
        Preconditions.checkNotNull(repository);

        TransactionMode transactionMode;
        if (!repository.isWritable()) {
            transactionMode = TransactionMode.READ_ONLY;
        } else {
            transactionMode = TransactionMode.WRITABLE_MANUAL_CLOSURE; // TODO Hack
        }
        // } else if (repository.getInferenceMode().isForwardEnabled()) {
        // transactionMode = TransactionMode.WRITABLE_AUTO_CLOSURE;
        // } else {
        // transactionMode = TransactionMode.WRITABLE_MANUAL_CLOSURE;
        // }

        this.id = id;
        this.repository = repository;
        this.parserConfig = SpringlesConnectionBase.DEFAULT_PARSER_CONFIG;
        this.inferenceMode = repository.getInferenceMode();

        this.status = Status.IDLE;
        this.pendingTransactions = Maps.newHashMap();
        this.closeLatch = null;

        this.currentTransactionMode = transactionMode;
        this.autoCommit = true;
        this.lastTransaction = null;
        this.lastTransactionMode = null;
    }

    // CONSTANT PROPERTIES

    @Override
    public String getID()
    {
        return this.id;
    }

    /**
     * {@inheritDoc} This method can be overridden by subclasses in order to return a more
     * specific type of repository.
     */
    @Override
    public SpringlesRepository getRepository()
    {
        return this.repository;
    }

    @Override
    public final ValueFactory getValueFactory()
    {
        return this.repository.getValueFactory();
    }

    // STATUS MANAGEMENT

    @Override
    public final boolean isOpen()
    {
        return this.status != Status.CLOSED;
    }

    @Override
    public final void close() throws RepositoryException
    {
        CountDownLatch latchToWaitFor = null;
        List<Transaction> transactionsToEnd = null;
        boolean notifyRepository = false;

        synchronized (this) {
            if (this.status == Status.CLOSED) {
                return;
            } else if (this.status != Status.CLOSING) {
                this.status = Status.CLOSING;
                if (!this.pendingTransactions.isEmpty()) {
                    transactionsToEnd = Lists.newArrayList(this.pendingTransactions.values());
                    this.closeLatch = new CountDownLatch(transactionsToEnd.size());
                }
                notifyRepository = true;
            }
            latchToWaitFor = this.closeLatch;
        }

        if (transactionsToEnd != null) {
            SpringlesConnectionBase.LOGGER.info("[{}] Forcing rollback of {} pending transaction",
                    this.id, transactionsToEnd.size());
            for (final Transaction transactionToEnd : transactionsToEnd) {
                try {
                    transactionToEnd.end(false);
                } catch (final Throwable ex) {
                    SpringlesConnectionBase.LOGGER
                            .error("Got exception while forcing transaction rollback", ex);
                }
            }
        }

        try {
            if (latchToWaitFor != null) {
                latchToWaitFor.await();
            }
        } catch (final InterruptedException ex) {
            throw new RepositoryException(
                    "Interrupted while waiting for connection close operation to complete", ex);
        } finally {
            this.status = Status.CLOSED;
        }

        if (notifyRepository) {
            this.repository.onConnectionClosed(this);
        }
    }

    private synchronized void registerPendingTransaction(final String transactionID,
            final Transaction transaction, final boolean writeTransaction)
    {
        this.pendingTransactions.put(transactionID, transaction);
        this.status = writeTransaction ? Status.READING : Status.WRITING;
    }

    private synchronized void unregisterPendingTransaction(final String transactionID)
    {
        this.pendingTransactions.remove(transactionID);
        if (this.pendingTransactions.isEmpty()
                && (this.status == Status.READING || this.status == Status.WRITING)) {
            this.status = Status.IDLE;
        }
        if (this.closeLatch != null) {
            this.closeLatch.countDown();
        }
    }

    /**
     * Helper method throwing an exception if the transaction is closing / closed.
     *
     * @throws IllegalStateException
     *             if the transaction is closing / closed
     */
    protected final void checkAccessible() throws IllegalStateException
    {
        final Status status = this.status;
        if (status == Status.CLOSED || status == Status.CLOSING) {
            throw new IllegalStateException("Connection has been closed or is closing");
        }
    }

    // TRANSACTION HANDLING

    @Override
    public final TransactionMode getTransactionMode() throws RepositoryException
    {
        this.checkAccessible();
        return this.currentTransactionMode;
    }

    @Override
    public final void setTransactionMode(final TransactionMode transactionMode)
            throws RepositoryException
    {
        Preconditions.checkNotNull(transactionMode);
        this.checkAccessible();

        TransactionMode actualMode = transactionMode;
        if (transactionMode != TransactionMode.READ_ONLY && !this.repository.isWritable()) {
            actualMode = TransactionMode.READ_ONLY;
        } else if (transactionMode == TransactionMode.WRITABLE_AUTO_CLOSURE
                && !this.repository.getInferenceMode().isForwardEnabled()) {
            actualMode = TransactionMode.WRITABLE_MANUAL_CLOSURE;
        }

        if (actualMode != transactionMode) {
            SpringlesConnectionBase.LOGGER.warn("[" + this.id + "] Requested transaction mode "
                    + transactionMode + " is unsupported, falling back to " + actualMode);
        }

        this.currentTransactionMode = actualMode;

        SpringlesConnectionBase.LOGGER.info("[{}] Transaction mode set to {}", this.id,
                actualMode);
    }

    @Override
    public final synchronized boolean isAutoCommit() throws RepositoryException
    {
        this.checkAccessible();
        return this.autoCommit;
    }

    @Override
    public final void setAutoCommit(final boolean autoCommit) throws RepositoryException
    {
        Transaction transactionToEnd;

        synchronized (this) {
            this.checkAccessible();

            if (this.autoCommit == autoCommit) {
                return;
            }

            transactionToEnd = this.lastTransaction;

            this.autoCommit = autoCommit;
            this.lastTransaction = null;
            this.lastTransactionMode = null;

            SpringlesConnectionBase.LOGGER.info("[{}] Auto-commit set to {}", this.id, autoCommit);
        }

        if (transactionToEnd != null) {
            transactionToEnd.end(true);
            SpringlesConnectionBase.LOGGER.info(
                    "[{}] Transaction committed due to auto-commit switched on",
                    transactionToEnd.getID());
        }
    }

    @Override
    public final void commit() throws RepositoryException
    {
        this.endTransaction(true);
    }

    @Override
    public final void rollback() throws RepositoryException
    {
        this.endTransaction(false);
    }

    private void endTransaction(final boolean commit) throws RepositoryException
    {
        Transaction transactionToEnd;

        synchronized (this) {
            this.checkAccessible();
            if (this.autoCommit) {
                transactionToEnd = null;
            } else {
                transactionToEnd = this.lastTransaction;
                this.lastTransaction = null;
                this.lastTransactionMode = null;
            }
        }

        if (transactionToEnd != null) {
            transactionToEnd.end(commit);
            if (SpringlesConnectionBase.LOGGER.isInfoEnabled()) {
                SpringlesConnectionBase.LOGGER.info("[{}] Transaction manually {}",
                        transactionToEnd.getID(), commit ? "committed" : "rolled back");
            }
        }
    }

    // return a transaction. each operation should be performed by calling this method and then
    // doing a single call on the obtained object (this allows automatic handling of auto-commit);
    // if multiple operations have to be performed, use execute(...)

    protected final synchronized Transaction getTransaction(final boolean writeOperation)
            throws RepositoryException
    {
        this.checkAccessible();

        if (this.lastTransaction != null) {
            if (writeOperation && this.lastTransactionMode == TransactionMode.READ_ONLY) {
                throw new IllegalStateException(
                        "Cannot perform a write operation while the connection is busy in a "
                                + "read-only transaction. Must end the transaction before");
            }
            return this.lastTransaction;

        } else {
            if (writeOperation && this.currentTransactionMode == TransactionMode.READ_ONLY) {
                throw new IllegalStateException(
                        "Cannot perform a write operation while transaction mode is set to "
                                + this.currentTransactionMode);
            }

            // Optimization: downgrade transaction mode to READ-ONLY where possible.
            TransactionMode mode = this.currentTransactionMode;
            if (this.autoCommit && mode == TransactionMode.WRITABLE_MANUAL_CLOSURE
                    && !writeOperation) {
                mode = TransactionMode.READ_ONLY;
                SpringlesConnectionBase.LOGGER
                        .info("[{}] Transaction mode downgraded to {} for auto-committing, "
                                + "non auto-closure operation", this.id, mode);
            }
            final AtomicReference<String> idHolder = new AtomicReference<String>(null);
            final Transaction transaction = this.repository.getTransaction(mode, this.autoCommit,
                    new EndListener() {

                        @Override
                        public void transactionEnded(final boolean committed,
                                final ClosureStatus newClosureStatus)
                        {
                            final String id = idHolder.get();
                            if (id != null) {
                                SpringlesConnectionBase.this.unregisterPendingTransaction(id);
                            }
                        }

                    });
            idHolder.set(transaction.getID());

            this.registerPendingTransaction(transaction.getID(), transaction,
                    mode != TransactionMode.READ_ONLY);

            if (!this.autoCommit) {
                this.lastTransaction = transaction;
                this.lastTransactionMode = mode;
            }

            return transaction;
        }
    }

    /*
     * private Transaction getTransaction(final boolean writeOperation, final BindingSet
     * specification) throws RepositoryException { // final Options options =
     * Options.fromBindings(specification);
     *
     * if (writeOperation && options.getTransactionMode() == TransactionMode.READ_ONLY) { throw
     * new IllegalStateException("Cannot perform a write operation " +
     * "in the context of read-only transaction"); }
     *
     * return this.repository.getTransaction(options.getTransactionMode(), options.isAutoCommit(),
     * options.getTransactionName()); }
     */

    // NAMESPACE MANAGEMENT

    @Override
    public final RepositoryResult<Namespace> getNamespaces() throws RepositoryException
    {
        return new RepositoryResult<Namespace>(this.getTransaction(false).getNamespaces());
    }

    @Override
    public final String getNamespace(final String prefix) throws RepositoryException
    {
        Preconditions.checkNotNull(prefix);
        return this.getTransaction(false).getNamespace(prefix);
    }

    @Override
    public final void setNamespace(final String prefix, final String name)
            throws RepositoryException
    {
        Preconditions.checkNotNull(prefix);
        Preconditions.checkNotNull(name);
        this.getTransaction(true).setNamespace(prefix, name);
    }

    @Override
    public final void removeNamespace(final String prefix) throws RepositoryException
    {
        Preconditions.checkNotNull(prefix);
        this.getTransaction(true).setNamespace(prefix, null);
    }

    @Override
    public final void clearNamespaces() throws RepositoryException
    {
        this.getTransaction(true).clearNamespaces();
    }

    // QUERY HANDLING

    @Override
    public final Query prepareQuery(final QueryLanguage language, final String queryString)
            throws RepositoryException, MalformedQueryException
    {
        return this.prepareQuery(language, queryString, null);
    }

    @Override
    public final Query prepareQuery(final QueryLanguage language, final String queryString,
            @Nullable final String baseURI) throws RepositoryException, MalformedQueryException
    {
        this.checkAccessible();
        Preconditions.checkNotNull(queryString);

        // Code taken taken from HTTPRepositoryConnection.
        if (language.equals(QueryLanguage.SPARQL)) {
            final String strippedQuery = QueryParserUtil.removeSPARQLQueryProlog(queryString)
                    .toUpperCase();
            if (strippedQuery.startsWith("SELECT")) {
                return this.prepareTupleQuery(language, queryString, baseURI);
            } else if (strippedQuery.startsWith("ASK")) {
                return this.prepareBooleanQuery(language, queryString, baseURI);
            } else {
                return this.prepareGraphQuery(language, queryString, baseURI);
            }

        } else if (QueryLanguage.SERQL.equals(language)) {
            final String strippedQuery = queryString.replace('(', ' ').trim();
            if (strippedQuery.toUpperCase().startsWith("SELECT")) {
                return this.prepareTupleQuery(language, queryString, baseURI);
            } else {
                return this.prepareGraphQuery(language, queryString, baseURI);
            }

        } else if (SpringlesRepository.PROTOCOL.equals(language)) {
            return this.prepareTupleQuery(language, queryString, baseURI);

        } else {
            throw new UnsupportedOperationException(
                    "Operation unsupported for query language " + language);
        }
    }

    @Override
    public final TupleQuery prepareTupleQuery(final QueryLanguage language,
            final String queryString) throws RepositoryException, MalformedQueryException
    {
        return this.prepareTupleQuery(language, queryString, null);
    }

    @Override
    public final TupleQuery prepareTupleQuery(final QueryLanguage language,
            final String queryString, @Nullable final String baseURI)
            throws RepositoryException, MalformedQueryException
    {
        this.checkAccessible();
        return new UnpreparedTupleQuery(
                QuerySpec.from(QueryType.TUPLE, queryString, language, baseURI));
    }

    private class UnpreparedTupleQuery extends AbstractQuery implements TupleQuery
    {

        private final QuerySpec<TupleQueryResult> spec;

        public UnpreparedTupleQuery(final QuerySpec<TupleQueryResult> spec)
        {
            this.spec = spec;
        }

        @Override
        public TupleQueryResult evaluate() throws QueryEvaluationException
        {
            return SpringlesConnectionBase.this.evaluateQuery(this.spec, this.getDataset(),
                    this.getBindings(), this.getIncludeInferred(), this.getMaxExecutionTime());
        }

        @Override
        public void evaluate(final TupleQueryResultHandler handler)
                throws QueryEvaluationException, TupleQueryResultHandlerException
        {
            SpringlesConnectionBase.this.evaluateQuery(this.spec, this.getDataset(),
                    this.getBindings(), this.getIncludeInferred(), this.getMaxExecutionTime(),
                    handler);
        }

    }

    @Override
    public final GraphQuery prepareGraphQuery(final QueryLanguage language,
            final String queryString) throws RepositoryException, MalformedQueryException
    {
        return this.prepareGraphQuery(language, queryString, null);
    }

    @Override
    public final GraphQuery prepareGraphQuery(final QueryLanguage language,
            final String queryString, @Nullable final String baseURI)
            throws RepositoryException, MalformedQueryException
    {
        this.checkAccessible();
        return new UnpreparedGraphQuery(
                QuerySpec.from(QueryType.GRAPH, queryString, language, baseURI));
    }

    private class UnpreparedGraphQuery extends AbstractQuery implements GraphQuery
    {

        private final QuerySpec<GraphQueryResult> spec;

        public UnpreparedGraphQuery(final QuerySpec<GraphQueryResult> spec)
        {
            this.spec = spec;
        }

        @Override
        public GraphQueryResult evaluate() throws QueryEvaluationException
        {
            return SpringlesConnectionBase.this.evaluateQuery(this.spec, this.getDataset(),
                    this.getBindings(), this.getIncludeInferred(), this.getMaxExecutionTime());
        }

        @Override
        public void evaluate(final RDFHandler handler)
                throws QueryEvaluationException, RDFHandlerException
        {
            SpringlesConnectionBase.this.evaluateQuery(this.spec, this.getDataset(),
                    this.getBindings(), this.getIncludeInferred(), this.getMaxExecutionTime(),
                    handler);
        }

    }

    @Override
    public final BooleanQuery prepareBooleanQuery(final QueryLanguage language,
            final String queryString) throws RepositoryException, MalformedQueryException
    {
        return this.prepareBooleanQuery(language, queryString, null);
    }

    @Override
    public final BooleanQuery prepareBooleanQuery(final QueryLanguage language,
            final String queryString, @Nullable final String baseURI)
            throws RepositoryException, MalformedQueryException
    {
        this.checkAccessible();
        return new UnpreparedBooleanQuery(
                QuerySpec.from(QueryType.BOOLEAN, queryString, language, baseURI));
    }

    private class UnpreparedBooleanQuery extends AbstractQuery implements BooleanQuery
    {

        private final QuerySpec<Boolean> spec;

        public UnpreparedBooleanQuery(final QuerySpec<Boolean> spec)
        {
            this.spec = spec;
        }

        @Override
        public boolean evaluate() throws QueryEvaluationException
        {
            return SpringlesConnectionBase.this.evaluateQuery(this.spec, this.getDataset(),
                    this.getBindings(), this.getIncludeInferred(), this.getMaxExecutionTime());
        }

    }

    private void evaluateQuery(final QuerySpec<TupleQueryResult> query, final Dataset dataset,
            final BindingSet bindings, final boolean includeInferred, final int timeout,
            final TupleQueryResultHandler handler)
            throws QueryEvaluationException, TupleQueryResultHandlerException
    {
        Preconditions.checkNotNull(handler);
        SpringlesConnectionBase.LOGGER.info("{}", query.getString());
        try {
            if (SpringlesRepository.PROTOCOL.equals(query.getLanguage())) {
                // getTransaction(false, bindings).query(query, null, null, InferenceMode.NONE, 0,
                // handler);
            } else {
                this.getTransaction(false).query(query, dataset, bindings,
                        this.getActualInferenceMode(includeInferred), timeout, handler);
            }
        } catch (final QueryEvaluationException ex) {
            if (ex.getCause() instanceof TupleQueryResultHandlerException) {
                throw (TupleQueryResultHandlerException) ex.getCause();
            }
            throw ex;
        } catch (final RepositoryException ex) {
            throw new QueryEvaluationException(ex);
        } catch (final RuntimeException ex) {
            throw ex;
        }
    }

    private void evaluateQuery(final QuerySpec<GraphQueryResult> query, final Dataset dataset,
            final BindingSet bindings, final boolean includeInferred, final int timeout,
            final RDFHandler handler) throws QueryEvaluationException, RDFHandlerException
    {
        Preconditions.checkNotNull(handler);
        try {
            this.getTransaction(false).query(query, dataset, bindings,
                    this.getActualInferenceMode(includeInferred), timeout, handler);

        } catch (final QueryEvaluationException ex) {
            if (ex.getCause() instanceof RDFHandlerException) {
                throw (RDFHandlerException) ex.getCause();
            }
            throw ex;
        } catch (final RepositoryException ex) {
            throw new QueryEvaluationException(ex);
        } catch (final RuntimeException ex) {
            throw ex;
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T evaluateQuery(final QuerySpec<T> query, final Dataset dataset,
            final BindingSet bindings, final boolean includeInferred, final int timeout)
            throws QueryEvaluationException
    {
        SpringlesConnectionBase.LOGGER.info("{}", query.getString());
        try {
            if (SpringlesRepository.PROTOCOL.equals(query.getLanguage())) {
                return this.getTransaction(false).query(query, null, null, InferenceMode.NONE, 0);

            } else if (query.getString().toLowerCase().replaceAll("\\s+", "")
                    .equals("select?closurestatus{}")) {
                final List<String> variables = ImmutableList.of("closurestatus");
                final ClosureStatus cs = this.getClosureStatus();
                SpringlesConnectionBase.LOGGER.info("{}", cs);
                final Literal lit = SimpleValueFactory.getInstance().createLiteral(cs.toString());
                return (T) new IteratingTupleQueryResult(variables,
                        ImmutableList.of(new ListBindingSet(variables, lit)));

            } else if (query.getString().toLowerCase().replaceAll("\\s+", "")
                    .equals("select?listofruleset{}")) {
                final List<String> variables = ImmutableList.of("listofruleset");
                String list = "";
                Rulesets.load();
                int list_size = Rulesets.list().size();
                for (final Ruleset r : Rulesets.list()) {
                    if (list_size > 4) {
                        Rulesets.unregister(r.getID());
                    }
                    list_size--;
                }
                list_size = Rulesets.list().size();
                SpringlesConnectionBase.LOGGER.info("{}", list_size);
                if (list_size - 4 == 0) {
                    final Literal lit = SimpleValueFactory.getInstance()
                            .createLiteral("no-ruleset");
                    return (T) new IteratingTupleQueryResult(variables,
                            ImmutableList.of(new ListBindingSet(variables, lit)));
                }
                for (final Ruleset r : Rulesets.list()) {
                    if (list_size > 4) {
                        list += r.getID().stringValue() + "--"
                                + r.getURL().toString().replaceAll("file:", "") + "\n";
                    }
                    list_size--;
                }

                SpringlesConnectionBase.LOGGER.info("{}", list);
                final Literal lit = SimpleValueFactory.getInstance().createLiteral(list);
                return (T) new IteratingTupleQueryResult(variables,
                        ImmutableList.of(new ListBindingSet(variables, lit)));
            } else {
                return this.getTransaction(false).query(query, dataset, bindings,
                        this.getActualInferenceMode(includeInferred), timeout);
            }
        } catch (final QueryEvaluationException ex) {
            throw ex;
        } catch (final RepositoryException ex) {
            throw new QueryEvaluationException(ex);
        } catch (final RuntimeException ex) {
            throw ex;
        }
    }

    // UPDATE HANDLING

    @Override
    public final Update prepareUpdate(final QueryLanguage language, final String updateString)
            throws RepositoryException, MalformedQueryException
    {
        return this.prepareUpdate(language, updateString, null);
    }

    @Override
    public final Update prepareUpdate(final QueryLanguage language, final String updateString,
            @Nullable final String baseURI) throws RepositoryException, MalformedQueryException
    {
        this.checkAccessible();
        return new UnpreparedUpdate(UpdateSpec.from(updateString, language, baseURI));
    }

    private class UnpreparedUpdate extends AbstractUpdate
    {

        private final UpdateSpec spec;

        public UnpreparedUpdate(final UpdateSpec spec)
        {
            this.spec = spec;
        }

        @Override
        public void execute() throws UpdateExecutionException
        {
            SpringlesConnectionBase.this.executeUpdate(this.spec, this.getDataset(),
                    this.getBindings(), this.getIncludeInferred());
        }

    }

    private void executeUpdate(final UpdateSpec update, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final boolean includeInferred)
            throws UpdateExecutionException
    {
        try {
            if (SpringlesRepository.PROTOCOL.equals(update.getLanguage())) {
                // getTransaction(true, bindings).update(update, null, null, InferenceMode.NONE);
            } else {

                // TODO this is an hack...
                if (update.getExpressions().size() == 1
                        && update.getExpressions().get(0) instanceof Clear) {
                    final Clear clear = (Clear) update.getExpressions().get(0);
                    if (clear.getGraph() != null && clear.getGraph().getValue() instanceof IRI) {
                        final String command = ((IRI) clear.getGraph().getValue()).stringValue();
                        if (command.equals("springles:update-closure")) {
                            SpringlesConnectionBase.LOGGER.info(
                                    "Handling 'clear graph springles:update-closure' request");
                            this.clearClosure();
                            this.updateClosure();
                            return;
                        } else if (command.equals("springles:clear-closure")) {
                            SpringlesConnectionBase.LOGGER.info(
                                    "Handling 'clear graph springles:clear-closure' request");
                            this.clearClosure();
                            return;
                        } else if (command.equals("rdfpro:update-closure")) {
                            SpringlesConnectionBase.LOGGER
                                    .info("Handling 'clear graph rdfpro:update-closure' request");
                            RulesetsRDFPRO.load();
                            this.clearClosure();
                            this.updateClosure();
                            return;
                        } else if (command.equals("rdfpro:clear-closure")) {
                            SpringlesConnectionBase.LOGGER.info("Handling 'clear graph' request");
                            this.clearClosure();
                            return;
                        } else if (command.equals("springles:auto-closure")) {
                            SpringlesConnectionBase.LOGGER
                                    .info("Handling 'clear graph springles:auto-closure' request");
                            this.setTransactionMode(TransactionMode.WRITABLE_AUTO_CLOSURE);
                            return;
                        } else if (command.equals("springles:manual-closure")) {
                            SpringlesConnectionBase.LOGGER.info(
                                    "Handling 'clear graph springles:manual-closure' request");
                            this.setTransactionMode(TransactionMode.WRITABLE_MANUAL_CLOSURE);
                            return;
                        }
                    }
                }

                this.getTransaction(true).update(update, dataset, bindings,
                        this.getActualInferenceMode(includeInferred));
            }

        } catch (final UpdateExecutionException ex) {
            throw ex;
        } catch (final RepositoryException ex) {
            throw new UpdateExecutionException(ex);
        } catch (final RuntimeException ex) {
            throw ex;
        }
    }

    // READ METHODS

    @Override
    public final RepositoryResult<Resource> getContextIDs() throws RepositoryException
    {
        return this.getContextIDs(true);
    }

    @Override
    public final RepositoryResult<Resource> getContextIDs(final boolean includeInferred)
            throws RepositoryException
    {
        return new RepositoryResult<Resource>(this.getTransaction(false)
                .getContextIDs(this.getActualInferenceMode(includeInferred)));
    }

    @Override
    public final RepositoryResult<Statement> getStatements(@Nullable final Resource subj,
            @Nullable final IRI pred, @Nullable final Value obj, final boolean includeInferred,
            final Resource... contexts) throws RepositoryException
    {
        Preconditions.checkNotNull(contexts);
        return new RepositoryResult<Statement>(this.getTransaction(false).getStatements(subj, pred,
                obj, this.getActualInferenceMode(includeInferred), contexts));
    }

    @Override
    public final boolean hasStatement(final Statement statement, final boolean includeInferred,
            final Resource... contexts) throws RepositoryException
    {
        return this.hasStatement(statement.getSubject(), statement.getPredicate(),
                statement.getObject(), includeInferred, contexts);
    }

    @Override
    public final boolean hasStatement(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final boolean includeInferred, final Resource... contexts)
            throws RepositoryException
    {
        Preconditions.checkNotNull(contexts);
        return this.getTransaction(false).hasStatement(subj, pred, obj,
                this.getActualInferenceMode(includeInferred), contexts);
    }

    @Override
    public final void export(final RDFHandler handler, final Resource... contexts)
            throws RepositoryException, RDFHandlerException
    {
        this.exportStatements(null, null, null, false, handler, contexts);
    }

    @Override
    public final void exportStatements(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final boolean includeInferred, final RDFHandler handler,
            final Resource... contexts) throws RepositoryException, RDFHandlerException
    {
        Preconditions.checkNotNull(handler);
        Preconditions.checkNotNull(contexts);

        final InferenceMode mode = this.getActualInferenceMode(includeInferred);
        final boolean updateClosure = includeInferred && mode.isForwardEnabled();

        final Transaction transaction = this.getTransaction(false);
        transaction.execute(new Operation<Void, RDFHandlerException>() {

            @Override
            public Void execute() throws RDFHandlerException, RepositoryException
            {
                handler.startRDF();

                final CloseableIteration<? extends Namespace, RepositoryException> namespaces;
                namespaces = transaction.getNamespaces();
                try {
                    while (namespaces.hasNext()) {
                        final Namespace namespace = namespaces.next();
                        handler.handleNamespace(namespace.getPrefix(), namespace.getName());
                    }
                } finally {
                    namespaces.close();
                }

                final CloseableIteration<? extends Statement, RepositoryException> statements;
                statements = transaction.getStatements(subj, pred, obj, mode, contexts);
                try {
                    while (statements.hasNext()) {
                        handler.handleStatement(statements.next());
                    }
                } finally {
                    statements.close();
                }

                handler.endRDF();

                return null;
            }

        }, false, updateClosure);
    }

    @Override
    public final long size(final Resource... contexts) throws RepositoryException
    {
        return this.size(false, contexts);
    }

    @Override
    public final long size(final boolean includeInferred, final Resource... contexts)
            throws RepositoryException
    {
        Preconditions.checkNotNull(contexts);
        return this.getTransaction(false).size(this.getActualInferenceMode(includeInferred),
                contexts);
    }

    @Override
    public final boolean isEmpty() throws RepositoryException
    {
        return !this.hasStatement(null, null, null, false);
    }

    @Override
    public final boolean isEmpty(final boolean includeInferred) throws RepositoryException
    {
        return !this.hasStatement(null, null, null, includeInferred);
    }

    // ADD METHODS (FROM STREAMS)

    @Override
    public final ParserConfig getParserConfig()
    {
        this.checkAccessible();
        return this.parserConfig;
    }

    @Override
    public final void setParserConfig(final ParserConfig parserConfig)
    {
        this.checkAccessible();
        this.parserConfig = parserConfig != null ? parserConfig
                : SpringlesConnectionBase.DEFAULT_PARSER_CONFIG;
    }

    @Override
    public final void add(final InputStream stream, @Nullable final String baseURI,
            @Nullable final RDFFormat format, final Resource... contexts)
            throws IOException, RDFParseException, RepositoryException
    {
        this.addHelper(stream, baseURI, format, contexts);
    }

    @Override
    public final void add(final Reader reader, @Nullable final String baseURI,
            @Nullable final RDFFormat format, final Resource... contexts)
            throws IOException, RDFParseException, RepositoryException
    {
        this.addHelper(reader, baseURI, format, contexts);
    }

    @Override
    public final void add(final URL url, @Nullable final String baseURI,
            @Nullable final RDFFormat format, final Resource... contexts)
            throws IOException, RDFParseException, RepositoryException
    {
        this.addHelper(url, baseURI, format, contexts);
    }

    @Override
    public final void add(final File file, @Nullable final String baseURI,
            @Nullable final RDFFormat format, final Resource... contexts)
            throws IOException, RDFParseException, RepositoryException
    {
        this.addHelper(file, baseURI, format, contexts);
    }

    private void addHelper(final Object input, @Nullable final String baseURI,
            @Nullable final RDFFormat format, final Resource... contexts)
            throws IOException, RDFParseException, RepositoryException
    {
        Preconditions.checkNotNull(contexts);

        try {
            final RDFParseOptions options = new RDFParseOptions(format, baseURI, null,
                    this.parserConfig);

            final RDFSource<RDFParseException> source;
            if (input instanceof InputStream) {
                source = RDFSource.deserializeFrom((InputStream) input, options);
            } else if (input instanceof Reader) {
                source = RDFSource.deserializeFrom((Reader) input, options);
            } else if (input instanceof URL) {
                source = RDFSource.deserializeFrom((URL) input, options);
            } else if (input instanceof File) {
                source = RDFSource.deserializeFrom((File) input, options);
            } else {
                throw new Error("Unexpected input type: " + input.getClass().getName());
            }

            final Transaction transaction = this.getTransaction(true);
            transaction.execute(new Operation<Void, RDFParseException>() {

                @Override
                public Void execute() throws RDFParseException, RepositoryException
                {
                    try {
                        source.streamTo(new AbstractRDFHandler() {

                            private final List<Statement> buffer = Lists
                                    .newArrayListWithCapacity(SpringlesConnectionBase.BATCH_SIZE);

                            @Override
                            public void handleNamespace(final String prefix, final String uri)
                                    throws RDFHandlerException
                            {
                                try {
                                    transaction.setNamespace(prefix, uri);
                                } catch (final RepositoryException ex) {
                                    throw new RDFHandlerException(ex);
                                }
                            }

                            @Override
                            public void handleStatement(final Statement statement)
                                    throws RDFHandlerException
                            {
                                this.buffer.add(statement);
                                if (this.buffer.size() == SpringlesConnectionBase.BATCH_SIZE) {
                                    this.flush();
                                }
                            }

                            @Override
                            public void endRDF() throws RDFHandlerException
                            {
                                if (!this.buffer.isEmpty()) {
                                    this.flush();
                                }
                            };

                            private void flush() throws RDFHandlerException
                            {
                                try {

                                    transaction.add(this.buffer, contexts);

                                } catch (final RepositoryException ex) {
                                    throw new RDFHandlerException(ex);
                                }
                                this.buffer.clear();
                            }

                        });

                    } catch (final RDFHandlerException ex) {
                        throw (RepositoryException) ex.getCause();
                    }

                    return null;
                }

            }, true, false);

        } catch (final RDFParseException ex) {
            if (ex.getCause() instanceof IOException) {
                throw (IOException) ex.getCause();
            }
            throw ex;
        }
    }

    // ADD METHODS (FROM STATEMENTS)

    @Override
    public final void add(final Resource subj, final IRI pred, final Value obj,
            final Resource... contexts) throws RepositoryException
    {
        final Statement stmt = SimpleValueFactory.getInstance().createStatement(subj, pred, obj);
        this.add(Collections.singleton(stmt), contexts);
    }

    @Override
    public final void add(final Statement statement, final Resource... contexts)
            throws RepositoryException
    {
        Preconditions.checkNotNull(statement);
        this.add(Collections.singleton(statement), contexts);
    }

    @Override
    public final <E extends Exception> void add(final Iteration<? extends Statement, E> statements,
            final Resource... contexts) throws RepositoryException, E
    {
        this.add(Iterations.asIterable(statements), contexts);
    }

    @Override
    public final void add(final Iterable<? extends Statement> statements,
            final Resource... contexts) throws RepositoryException
    {
        Preconditions.checkNotNull(statements);
        Preconditions.checkNotNull(contexts);
        this.getTransaction(true).add(statements, contexts);
    }

    // REMOVE METHODS

    @Override
    public final void remove(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final Resource... contexts) throws RepositoryException
    {
        Preconditions.checkNotNull(contexts);
        this.getTransaction(true).remove(subj, pred, obj, contexts);
    }

    @Override
    public final void remove(final Statement statement, final Resource... contexts)
            throws RepositoryException
    {
        this.remove(Collections.singleton(statement), contexts);
    }

    @Override
    public final <E extends Exception> void remove(
            final Iteration<? extends Statement, E> statements, final Resource... contexts)
            throws RepositoryException, E
    {
        // Must read all elements as the iteration may come from a query to this repository.
        this.remove(Iterations.getAllElements(statements), contexts);
    }

    @Override
    public final void remove(final Iterable<? extends Statement> statements,
            final Resource... contexts) throws RepositoryException
    {
        Preconditions.checkNotNull(statements);
        Preconditions.checkNotNull(contexts);
        this.getTransaction(true).remove(statements, contexts);
    }

    @Override
    public final void clear(final Resource... contexts) throws RepositoryException
    {
        this.remove(null, null, null, contexts);
    }

    @Override
    public final void reset() throws RepositoryException
    {
        this.getTransaction(true).reset();
    }

    // INFERENCE MANAGEMENT

    @Override
    public final InferenceMode getInferenceMode() throws RepositoryException
    {
        this.checkAccessible();
        return this.inferenceMode;
    }

    @Override
    public final void setInferenceMode(final InferenceMode inferenceMode)
            throws RepositoryException
    {
        Preconditions.checkNotNull(inferenceMode);
        this.checkAccessible();

        final InferenceMode actualMode = InferenceMode.intersect(inferenceMode,
                this.repository.getInferenceMode());

        if (actualMode != inferenceMode && SpringlesConnectionBase.LOGGER.isWarnEnabled()) {
            SpringlesConnectionBase.LOGGER.warn("[" + this.id + "] Requested inference mode "
                    + inferenceMode + " is unsupported, falling back to " + actualMode);
        }

        if (actualMode != this.inferenceMode) {
            this.inferenceMode = actualMode;
            SpringlesConnectionBase.LOGGER.info("[{}] Inference mode set to {}", actualMode);
        }
    }

    private InferenceMode getActualInferenceMode(final boolean includeInferred)
            throws RepositoryException
    {
        return includeInferred ? this.inferenceMode : InferenceMode.NONE;
    }

    @Override
    public final ClosureStatus getClosureStatus() throws RepositoryException
    {
        return this.getTransaction(false).getClosureStatus();
    }

    @Override
    public final void updateClosure() throws RepositoryException
    {
        this.getTransaction(true).updateClosure();
    }

    @Override
    public final void clearClosure() throws RepositoryException
    {
        this.getTransaction(true).clearClosure();
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this).add("id", this.id)
                .add("autoCommit", this.autoCommit)
                .add("transactionMode", this.currentTransactionMode)
                .add("inferenceMode", this.inferenceMode).toString();
    }

    @Override
    public void begin() throws RepositoryException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void begin(final IsolationLevel level) throws RepositoryException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean isActive() throws UnknownTransactionStateException, RepositoryException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setIsolationLevel(final IsolationLevel level) throws IllegalStateException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public IsolationLevel getIsolationLevel()
    {
        // TODO Auto-generated method stub
        return null;
    }

}
