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

import org.openrdf.IsolationLevel;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.algebra.Clear;
import org.openrdf.query.impl.AbstractQuery;
import org.openrdf.query.impl.AbstractUpdate;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.UnknownTransactionStateException;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

//import eu.fbk.dkm.internal.springles.protocol.Options;
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
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iteration;

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

    private static final ParserConfig DEFAULT_PARSER_CONFIG = new ParserConfig(true, false, false,
            DatatypeHandling.VERIFY);

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
//        } else if (repository.getInferenceMode().isForwardEnabled()) {
//            transactionMode = TransactionMode.WRITABLE_AUTO_CLOSURE;
//        } else {
//            transactionMode = TransactionMode.WRITABLE_MANUAL_CLOSURE;
//        }

        this.id = id;
        this.repository = repository;
        this.parserConfig = DEFAULT_PARSER_CONFIG;
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
            LOGGER.debug("[{}] Forcing rollback of {} pending transaction", this.id,
                    transactionsToEnd.size());
            for (final Transaction transactionToEnd : transactionsToEnd) {
                try {
                    transactionToEnd.end(false);
                } catch (final Throwable ex) {
                    LOGGER.error("Got exception while forcing transaction rollback", ex);
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
        checkAccessible();
        return this.currentTransactionMode;
    }

    @Override
    public final void setTransactionMode(final TransactionMode transactionMode)
            throws RepositoryException
    {
        Preconditions.checkNotNull(transactionMode);
        checkAccessible();

        TransactionMode actualMode = transactionMode;
        if (transactionMode != TransactionMode.READ_ONLY && !this.repository.isWritable()) {
            actualMode = TransactionMode.READ_ONLY;
        } else if (transactionMode == TransactionMode.WRITABLE_AUTO_CLOSURE
                && !this.repository.getInferenceMode().isForwardEnabled()) {
            actualMode = TransactionMode.WRITABLE_MANUAL_CLOSURE;
        }

        if (actualMode != transactionMode) {
            LOGGER.warn("[" + this.id + "] Requested transaction mode " + transactionMode
                    + " is unsupported, falling back to " + actualMode);
        }

        this.currentTransactionMode = actualMode;

        LOGGER.debug("[{}] Transaction mode set to {}", this.id, actualMode);
    }

    @Override
    public final synchronized boolean isAutoCommit() throws RepositoryException
    {
        checkAccessible();
        return this.autoCommit;
    }

    @Override
    public final void setAutoCommit(final boolean autoCommit) throws RepositoryException
    {
        Transaction transactionToEnd;

        synchronized (this) {
            checkAccessible();

            if (this.autoCommit == autoCommit) {
                return;
            }

            transactionToEnd = this.lastTransaction;

            this.autoCommit = autoCommit;
            this.lastTransaction = null;
            this.lastTransactionMode = null;

            LOGGER.debug("[{}] Auto-commit set to {}", this.id, autoCommit);
        }

        if (transactionToEnd != null) {
            transactionToEnd.end(true);
            LOGGER.debug("[{}] Transaction committed due to auto-commit switched on",
                    transactionToEnd.getID());
        }
    }

    @Override
    public final void commit() throws RepositoryException
    {
        endTransaction(true);
    }

    @Override
    public final void rollback() throws RepositoryException
    {
        endTransaction(false);
    }

    private void endTransaction(final boolean commit) throws RepositoryException
    {
        Transaction transactionToEnd;

        synchronized (this) {
            checkAccessible();
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
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] Transaction manually {}", transactionToEnd.getID(),
                        commit ? "committed" : "rolled back");
            }
        }
    }

    // return a transaction. each operation should be performed by calling this method and then
    // doing a single call on the obtained object (this allows automatic handling of auto-commit);
    // if multiple operations have to be performed, use execute(...)

    protected final synchronized Transaction getTransaction(final boolean writeOperation)
            throws RepositoryException
    {
        checkAccessible();

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
                LOGGER.debug("[{}] Transaction mode downgraded to {} for auto-committing, "
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
                                unregisterPendingTransaction(id);
                            }
                        }

                    });
            idHolder.set(transaction.getID());

            registerPendingTransaction(transaction.getID(), transaction,
                    mode != TransactionMode.READ_ONLY);

            if (!this.autoCommit) {
                this.lastTransaction = transaction;
                this.lastTransactionMode = mode;
            }

            return transaction;
        }
    }

  /*  private Transaction getTransaction(final boolean writeOperation, final BindingSet specification)
            throws RepositoryException
    {
  //      final Options options = Options.fromBindings(specification);

        if (writeOperation && options.getTransactionMode() == TransactionMode.READ_ONLY) {
            throw new IllegalStateException("Cannot perform a write operation "
                    + "in the context of read-only transaction");
        }

        return this.repository.getTransaction(options.getTransactionMode(),
                options.isAutoCommit(), options.getTransactionName());
    }*/

    // NAMESPACE MANAGEMENT

    @Override
    public final RepositoryResult<Namespace> getNamespaces() throws RepositoryException
    {
        return new RepositoryResult<Namespace>(getTransaction(false).getNamespaces());
    }

    @Override
    public final String getNamespace(final String prefix) throws RepositoryException
    {
        Preconditions.checkNotNull(prefix);
        return getTransaction(false).getNamespace(prefix);
    }

    @Override
    public final void setNamespace(final String prefix, final String name)
            throws RepositoryException
    {
        Preconditions.checkNotNull(prefix);
        Preconditions.checkNotNull(name);
        getTransaction(true).setNamespace(prefix, name);
    }

    @Override
    public final void removeNamespace(final String prefix) throws RepositoryException
    {
        Preconditions.checkNotNull(prefix);
        getTransaction(true).setNamespace(prefix, null);
    }

    @Override
    public final void clearNamespaces() throws RepositoryException
    {
        getTransaction(true).clearNamespaces();
    }

    // QUERY HANDLING

    @Override
    public final Query prepareQuery(final QueryLanguage language, final String queryString)
            throws RepositoryException, MalformedQueryException
    {
        return prepareQuery(language, queryString, null);
    }

    @Override
    public final Query prepareQuery(final QueryLanguage language, final String queryString,
            @Nullable final String baseURI) throws RepositoryException, MalformedQueryException
    {
        checkAccessible();
        Preconditions.checkNotNull(queryString);

        // Code taken taken from HTTPRepositoryConnection.
        if (language.equals(QueryLanguage.SPARQL)) {
            final String strippedQuery = QueryParserUtil.removeSPARQLQueryProlog(queryString)
                    .toUpperCase();
            if (strippedQuery.startsWith("SELECT")) {
                return prepareTupleQuery(language, queryString, baseURI);
            } else if (strippedQuery.startsWith("ASK")) {
                return prepareBooleanQuery(language, queryString, baseURI);
            } else {
                return prepareGraphQuery(language, queryString, baseURI);
            }

        } else if (QueryLanguage.SERQL.equals(language)) {
            final String strippedQuery = queryString.replace('(', ' ').trim();
            if (strippedQuery.toUpperCase().startsWith("SELECT")) {
                return prepareTupleQuery(language, queryString, baseURI);
            } else {
                return prepareGraphQuery(language, queryString, baseURI);
            }

        } else if (SpringlesRepository.PROTOCOL.equals(language)) {
            return prepareTupleQuery(language, queryString, baseURI);

        } else {
            throw new UnsupportedOperationException("Operation unsupported for query language "
                    + language);
        }
    }

    @Override
    public final TupleQuery prepareTupleQuery(final QueryLanguage language,
            final String queryString) throws RepositoryException, MalformedQueryException
    {
        return prepareTupleQuery(language, queryString, null);
    }

    @Override
    public final TupleQuery prepareTupleQuery(final QueryLanguage language,
            final String queryString, @Nullable final String baseURI) throws RepositoryException,
            MalformedQueryException
    {
        checkAccessible();
        return new UnpreparedTupleQuery(QuerySpec.from(QueryType.TUPLE, queryString, language,
                baseURI));
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
            return evaluateQuery(this.spec, getDataset(), getBindings(), getIncludeInferred(),
                    getMaxQueryTime());
        }

        @Override
        public void evaluate(final TupleQueryResultHandler handler)
                throws QueryEvaluationException, TupleQueryResultHandlerException
        {
            evaluateQuery(this.spec, getDataset(), getBindings(), getIncludeInferred(),
                    getMaxQueryTime(), handler);
        }

    }

    @Override
    public final GraphQuery prepareGraphQuery(final QueryLanguage language,
            final String queryString) throws RepositoryException, MalformedQueryException
    {
        return prepareGraphQuery(language, queryString, null);
    }

    @Override
    public final GraphQuery prepareGraphQuery(final QueryLanguage language,
            final String queryString, @Nullable final String baseURI) throws RepositoryException,
            MalformedQueryException
    {
        checkAccessible();
        return new UnpreparedGraphQuery(QuerySpec.from(QueryType.GRAPH, queryString, language,
                baseURI));
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
            return evaluateQuery(this.spec, getDataset(), getBindings(), getIncludeInferred(),
                    getMaxQueryTime());
        }

        @Override
        public void evaluate(final RDFHandler handler) throws QueryEvaluationException,
                RDFHandlerException
        {
            evaluateQuery(this.spec, getDataset(), getBindings(), getIncludeInferred(),
                    getMaxQueryTime(), handler);
        }

    }

    @Override
    public final BooleanQuery prepareBooleanQuery(final QueryLanguage language,
            final String queryString) throws RepositoryException, MalformedQueryException
    {
        return prepareBooleanQuery(language, queryString, null);
    }

    @Override
    public final BooleanQuery prepareBooleanQuery(final QueryLanguage language,
            final String queryString, @Nullable final String baseURI) throws RepositoryException,
            MalformedQueryException
    {
        checkAccessible();
        return new UnpreparedBooleanQuery(QuerySpec.from(QueryType.BOOLEAN, queryString, language,
                baseURI));
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
            return evaluateQuery(this.spec, getDataset(), getBindings(), getIncludeInferred(),
                    getMaxQueryTime());
        }

    }

    private void evaluateQuery(final QuerySpec<TupleQueryResult> query, final Dataset dataset,
            final BindingSet bindings, final boolean includeInferred, final int timeout,
            final TupleQueryResultHandler handler) throws QueryEvaluationException,
            TupleQueryResultHandlerException
    {
        Preconditions.checkNotNull(handler);
        try {
            if (SpringlesRepository.PROTOCOL.equals(query.getLanguage())) {
            //    getTransaction(false, bindings).query(query, null, null, InferenceMode.NONE, 0,
              //          handler);
            } else {
                getTransaction(false).query(query, dataset, bindings,
                        getActualInferenceMode(includeInferred), timeout, handler);
            }
        } catch (final QueryEvaluationException ex) {
            if (ex.getCause() instanceof TupleQueryResultHandlerException) {
                throw (TupleQueryResultHandlerException) ex.getCause();
            }
            throw ex;
        } catch (final RuntimeException ex) {
            throw ex;
        } catch (final RepositoryException ex) {
            throw new QueryEvaluationException(ex);
        }
    }

    private void evaluateQuery(final QuerySpec<GraphQueryResult> query, final Dataset dataset,
            final BindingSet bindings, final boolean includeInferred, final int timeout,
            final RDFHandler handler) throws QueryEvaluationException, RDFHandlerException
    {
        Preconditions.checkNotNull(handler);
        try {
            getTransaction(false).query(query, dataset, bindings,
                    getActualInferenceMode(includeInferred), timeout, handler);

        } catch (final QueryEvaluationException ex) {
            if (ex.getCause() instanceof RDFHandlerException) {
                throw (RDFHandlerException) ex.getCause();
            }
            throw ex;
        } catch (final RuntimeException ex) {
            throw ex;
        } catch (final RepositoryException ex) {
            throw new QueryEvaluationException(ex);
        }
    }

    private <T> T evaluateQuery(final QuerySpec<T> query, final Dataset dataset,
            final BindingSet bindings, final boolean includeInferred, final int timeout)
            throws QueryEvaluationException
    {
        try {
            if (SpringlesRepository.PROTOCOL.equals(query.getLanguage())) {
               return getTransaction(false).query(query, null, null,
                       InferenceMode.NONE, 0);
            } else {
                return getTransaction(false).query(query, dataset, bindings,
                        getActualInferenceMode(includeInferred), timeout);
            }
        } catch (final QueryEvaluationException ex) {
            throw ex;
        } catch (final RuntimeException ex) {
            throw ex;
        } catch (final RepositoryException ex) {
            throw new QueryEvaluationException(ex);
        }
    }

    // UPDATE HANDLING

    @Override
    public final Update prepareUpdate(final QueryLanguage language, final String updateString)
            throws RepositoryException, MalformedQueryException
    {
        return prepareUpdate(language, updateString, null);
    }

    @Override
    public final Update prepareUpdate(final QueryLanguage language, final String updateString,
            @Nullable final String baseURI) throws RepositoryException, MalformedQueryException
    {
        checkAccessible();
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
            executeUpdate(this.spec, getDataset(), getBindings(), getIncludeInferred());
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
                    if (clear.getGraph() != null && clear.getGraph().getValue() instanceof URI) {
                        final String command = ((URI) clear.getGraph().getValue()).stringValue();
                        if (command.equals("springles:update-closure")) {
                            LOGGER.info("Handling 'clear graph springles:update-closure' request");
                            clearClosure();
                            updateClosure();
                            return;
                        }else if (command.equals("springles:clear-closure")) {
                            LOGGER.info("Handling 'clear graph springles:clear-closure' request");
                            clearClosure();
                            return;
                        }else if (command.equals("rdfpro:update-closure")) {
                            LOGGER.info("Handling 'clear graph rdfpro:update-closure' request");
                            clearClosure();
                            updateClosure();
                            return;
                        }else if (command.equals("springles:auto-closure")) {
                            LOGGER.info("Handling 'clear graph springles:auto-closure' request");
                            setTransactionMode(TransactionMode.WRITABLE_AUTO_CLOSURE);
                            return;
                        } else if (command.equals("springles:manual-closure")) {
                            LOGGER.info("Handling 'clear graph springles:manual-closure' request");
                            setTransactionMode(TransactionMode.WRITABLE_MANUAL_CLOSURE);
                            return;
                        }
                    }
                }

                getTransaction(true).update(update, dataset, bindings,
                        getActualInferenceMode(includeInferred));
            }

        } catch (final UpdateExecutionException ex) {
            throw ex;
        } catch (final RuntimeException ex) {
            throw ex;
        } catch (final RepositoryException ex) {
            throw new UpdateExecutionException(ex);
        }
    }

    // READ METHODS

    @Override
    public final RepositoryResult<Resource> getContextIDs() throws RepositoryException
    {
        return getContextIDs(true);
    }

    @Override
    public final RepositoryResult<Resource> getContextIDs(final boolean includeInferred)
            throws RepositoryException
    {
        return new RepositoryResult<Resource>(getTransaction(false).getContextIDs(
                getActualInferenceMode(includeInferred)));
    }

    @Override
    public final RepositoryResult<Statement> getStatements(@Nullable final Resource subj,
            @Nullable final URI pred, @Nullable final Value obj, final boolean includeInferred,
            final Resource... contexts) throws RepositoryException
    {
        Preconditions.checkNotNull(contexts);
        return new RepositoryResult<Statement>(getTransaction(false).getStatements(subj, pred,
                obj, getActualInferenceMode(includeInferred), contexts));
    }

    @Override
    public final boolean hasStatement(final Statement statement, final boolean includeInferred,
            final Resource... contexts) throws RepositoryException
    {
        return hasStatement(statement.getSubject(), statement.getPredicate(),
                statement.getObject(), includeInferred, contexts);
    }

    @Override
    public final boolean hasStatement(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final boolean includeInferred, final Resource... contexts)
            throws RepositoryException
    {
        Preconditions.checkNotNull(contexts);
        return getTransaction(false).hasStatement(subj, pred, obj,
                getActualInferenceMode(includeInferred), contexts);
    }

    @Override
    public final void export(final RDFHandler handler, final Resource... contexts)
            throws RepositoryException, RDFHandlerException
    {
        exportStatements(null, null, null, false, handler, contexts);
    }

    @Override
    public final void exportStatements(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final boolean includeInferred, final RDFHandler handler,
            final Resource... contexts) throws RepositoryException, RDFHandlerException
    {
        Preconditions.checkNotNull(handler);
        Preconditions.checkNotNull(contexts);

        final InferenceMode mode = getActualInferenceMode(includeInferred);
        final boolean updateClosure = includeInferred && mode.isForwardEnabled();

        final Transaction transaction = getTransaction(false);
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
        return size(false, contexts);
    }

    @Override
    public final long size(final boolean includeInferred, final Resource... contexts)
            throws RepositoryException
    {
        Preconditions.checkNotNull(contexts);
        return getTransaction(false).size(getActualInferenceMode(includeInferred), contexts);
    }

    @Override
    public final boolean isEmpty() throws RepositoryException
    {
        return !hasStatement(null, null, null, false);
    }

    @Override
    public final boolean isEmpty(final boolean includeInferred) throws RepositoryException
    {
        return !hasStatement(null, null, null, includeInferred);
    }

    // ADD METHODS (FROM STREAMS)

    @Override
    public final ParserConfig getParserConfig()
    {
        checkAccessible();
        return this.parserConfig;
    }

    @Override
    public final void setParserConfig(final ParserConfig parserConfig)
    {
        checkAccessible();
        this.parserConfig = parserConfig != null ? parserConfig : DEFAULT_PARSER_CONFIG;
    }

    @Override
    public final void add(final InputStream stream, @Nullable final String baseURI,
            @Nullable final RDFFormat format, final Resource... contexts) throws IOException,
            RDFParseException, RepositoryException
    {
        addHelper(stream, baseURI, format, contexts);
    }

    @Override
    public final void add(final Reader reader, @Nullable final String baseURI,
            @Nullable final RDFFormat format, final Resource... contexts) throws IOException,
            RDFParseException, RepositoryException
    {
        addHelper(reader, baseURI, format, contexts);
    }

    @Override
    public final void add(final URL url, @Nullable final String baseURI,
            @Nullable final RDFFormat format, final Resource... contexts) throws IOException,
            RDFParseException, RepositoryException
    {
        addHelper(url, baseURI, format, contexts);
    }

    @Override
    public final void add(final File file, @Nullable final String baseURI,
            @Nullable final RDFFormat format, final Resource... contexts) throws IOException,
            RDFParseException, RepositoryException
    {
        addHelper(file, baseURI, format, contexts);
    }

    private void addHelper(final Object input, @Nullable final String baseURI,
            @Nullable final RDFFormat format, final Resource... contexts) throws IOException,
            RDFParseException, RepositoryException
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

            final Transaction transaction = getTransaction(true);
            transaction.execute(new Operation<Void, RDFParseException>() {

                @Override
                public Void execute() throws RDFParseException, RepositoryException
                {
                    try {
                        source.streamTo(new RDFHandlerBase() {

                            private final List<Statement> buffer = Lists
                                    .newArrayListWithCapacity(BATCH_SIZE);

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
                                if (this.buffer.size() == BATCH_SIZE) {
                                    flush();
                                }
                            }

                            @Override
                            public void endRDF() throws RDFHandlerException
                            {
                                if (!this.buffer.isEmpty()) {
                                    flush();
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
    public final void add(final Resource subj, final URI pred, final Value obj,
            final Resource... contexts) throws RepositoryException
    {
        add(Collections.singleton(new StatementImpl(subj, pred, obj)), contexts);
    }

    @Override
    public final void add(final Statement statement, final Resource... contexts)
            throws RepositoryException
    {
        Preconditions.checkNotNull(statement);
        add(Collections.singleton(statement), contexts);
    }

    @Override
    public final <E extends Exception> void add(
            final Iteration<? extends Statement, E> statements, final Resource... contexts)
            throws RepositoryException, E
    {
        add(Iterations.asIterable(statements), contexts);
    }

    @Override
    public final void add(final Iterable<? extends Statement> statements,
            final Resource... contexts) throws RepositoryException
    {
        Preconditions.checkNotNull(statements);
        Preconditions.checkNotNull(contexts);
        getTransaction(true).add(statements, contexts);
    }

    // REMOVE METHODS

    @Override
    public final void remove(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final Resource... contexts) throws RepositoryException
    {
        Preconditions.checkNotNull(contexts);
        getTransaction(true).remove(subj, pred, obj, contexts);
    }

    @Override
    public final void remove(final Statement statement, final Resource... contexts)
            throws RepositoryException
    {
        remove(Collections.singleton(statement), contexts);
    }

    @Override
    public final <E extends Exception> void remove(
            final Iteration<? extends Statement, E> statements, final Resource... contexts)
            throws RepositoryException, E
    {
        // Must read all elements as the iteration may come from a query to this repository.
        remove(Iterations.getAllElements(statements), contexts);
    }

    @Override
    public final void remove(final Iterable<? extends Statement> statements,
            final Resource... contexts) throws RepositoryException
    {
        Preconditions.checkNotNull(statements);
        Preconditions.checkNotNull(contexts);
        getTransaction(true).remove(statements, contexts);
    }

    @Override
    public final void clear(final Resource... contexts) throws RepositoryException
    {
        remove(null, null, null, contexts);
    }

    @Override
    public final void reset() throws RepositoryException
    {
        getTransaction(true).reset();
    }

    // INFERENCE MANAGEMENT

    @Override
    public final InferenceMode getInferenceMode() throws RepositoryException
    {
        checkAccessible();
        return this.inferenceMode;
    }

    @Override
    public final void setInferenceMode(final InferenceMode inferenceMode)
            throws RepositoryException
    {
        Preconditions.checkNotNull(inferenceMode);
        checkAccessible();

        final InferenceMode actualMode = InferenceMode.intersect(inferenceMode,
                this.repository.getInferenceMode());

        if (actualMode != inferenceMode && LOGGER.isWarnEnabled()) {
            LOGGER.warn("[" + this.id + "] Requested inference mode " + inferenceMode
                    + " is unsupported, falling back to " + actualMode);
        }

        if (actualMode != this.inferenceMode) {
            this.inferenceMode = actualMode;
            LOGGER.debug("[{}] Inference mode set to {}", actualMode);
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
        return getTransaction(false).getClosureStatus();
    }

    @Override
    public final void updateClosure() throws RepositoryException
    {
        getTransaction(true).updateClosure();
    }

    @Override
    public final void clearClosure() throws RepositoryException
    {
        getTransaction(true).clearClosure();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this).add("id", this.id).add("autoCommit", this.autoCommit)
                .add("transactionMode", this.currentTransactionMode)
                .add("inferenceMode", this.inferenceMode).toString();
    }

	@Override
	public void begin() throws RepositoryException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isActive() throws UnknownTransactionStateException,
			RepositoryException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setIsolationLevel(IsolationLevel level)
			throws IllegalStateException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public IsolationLevel getIsolationLevel() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void begin(IsolationLevel level) throws RepositoryException {
		// TODO Auto-generated method stub
		
	}

}
