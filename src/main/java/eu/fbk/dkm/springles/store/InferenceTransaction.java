package eu.fbk.dkm.springles.store;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResultUtil;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UpdateExpr;
import org.openrdf.query.impl.GraphQueryResultImpl;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import eu.fbk.dkm.internal.util.Algebra;
import eu.fbk.dkm.internal.util.Contexts;
import eu.fbk.dkm.internal.util.Iterations;
import eu.fbk.dkm.internal.util.SparqlRenderer;
import eu.fbk.dkm.internal.util.URIPrefix;
import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.dkm.springles.base.ForwardingTransaction;
import eu.fbk.dkm.springles.base.QuerySpec;
import eu.fbk.dkm.springles.base.QueryType;
import eu.fbk.dkm.springles.base.Transaction;
import eu.fbk.dkm.springles.base.UpdateSpec;
import eu.fbk.dkm.springles.inferencer.Inferencer;
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;

// LIMITATIONS
// - backward reasoning cannot introduce new contexts (see ReadTransaction, can be easily removed)
// - operation that read/check for statements in RepositoryConnection cannot reference BNodes and
// at the same time ask for inclusion of inferred statements with backward reasoning enabled
// (rationale: this would require executing a rewritten SPARQL query that matches BNodes, which is
// not possible).

class InferenceTransaction extends ForwardingTransaction
{

    private static final Logger LOGGER = LoggerFactory.getLogger(InferenceTransaction.class);

    private static final Cache<Inferencer, ClosureStatus> CLOSURE_STATUS_CACHE = CacheBuilder
            .newBuilder().weakKeys().build();

    private final Transaction delegate;

    private final Inferencer inferencer;

    private final URIPrefix inferredContextPrefix;

    @Nullable
    private final ScheduledExecutorService scheduler;

    private final File closureMetadataFile;

    private final ClosureStatus originalClosureStatus;

    private ClosureStatus currentClosureStatus;

    private volatile InferenceController controller;

    public InferenceTransaction(final Transaction delegate, final Inferencer inferencer,
            final URIPrefix inferredContextURIPrefix,
            @Nullable final ScheduledExecutorService scheduler, final File closureMetadataFile)
            throws RepositoryException
    {
        Preconditions.checkNotNull(delegate);
        Preconditions.checkNotNull(inferencer);
        Preconditions.checkNotNull(inferredContextURIPrefix);

        this.delegate = delegate;
        this.inferencer = inferencer;
        this.inferredContextPrefix = inferredContextURIPrefix;
        this.scheduler = scheduler;
        this.closureMetadataFile = closureMetadataFile;
        this.controller = null;

        ClosureStatus status = CLOSURE_STATUS_CACHE.getIfPresent(this.inferencer);
        if (status == null) {
            status = readClosureMetadata();
            CLOSURE_STATUS_CACHE.put(this.inferencer, status);
        }

        this.originalClosureStatus = status;
        this.currentClosureStatus = status;
    }

    @Override
    protected Transaction delegate()
    {
        return this.delegate;
    }

    private ClosureStatus readClosureMetadata() throws RepositoryException
    {
        if (this.closureMetadataFile == null || !this.closureMetadataFile.exists()) {
            LOGGER.info("[{}] Initial closure status set to {}", getID(), ClosureStatus.STALE);
            return ClosureStatus.STALE;
        }

        try {
            final String content = Files.toString(this.closureMetadataFile, Charsets.UTF_8);
            final String[] tokens = content.split(" ");
            final ClosureStatus status = ClosureStatus.valueOf(tokens[0]);
            final String digest = tokens[1];
            final boolean empty = Boolean.parseBoolean(tokens[2]);

            if (!digest.equals(this.inferencer.getConfigurationDigest())) {
                LOGGER.info("[{}] Inferencer configuration changed: initial closure status is {}",
                        getID(), ClosureStatus.STALE);
                return ClosureStatus.STALE;

            } else if (empty != !this.delegate.hasStatement(null, null, null, InferenceMode.NONE)) {
                LOGGER.info("[{}] Repository has been cleared (possibly transient repository): "
                        + "initial closure status is {}", getID(), ClosureStatus.STALE);
                return ClosureStatus.STALE;

            } else {
                LOGGER.info("[] Initial closure status load from file: {}", getID(), status);
                return status;
            }

        } catch (final IOException ex) {
            throw new RepositoryException("Could not read closure metadata from "
                    + this.closureMetadataFile, ex);
        }
    }

    private void writeClosureMetadata(final boolean emptyRepository) throws RepositoryException
    {
        if (this.closureMetadataFile == null) {
            LOGGER.info("[{}] Closure metadata not persisted for transient repository", getID());
            return;
        }

        try {
            final String content = this.currentClosureStatus.toString() + " "
                    + this.inferencer.getConfigurationDigest() + " " + emptyRepository;
            Files.write(content, this.closureMetadataFile, Charsets.UTF_8);
            LOGGER.info("[{}] Closure metadata saved to {}", getID(), this.closureMetadataFile);

        } catch (final IOException ex) {
            throw new RepositoryException("Could not write closure metadata to "
                    + this.closureMetadataFile, ex);
        }
    }

    private InferenceController getInferenceController(final boolean canCreate)
            throws RepositoryException
    {
    	//LOGGER.info("{} {} {} {}",this.inferencer,this.inferredContextPrefix,this.scheduler,this.currentClosureStatus);
        if (this.controller == null && canCreate) {
            synchronized (this) {
                if (this.controller == null) {
                    this.controller = new InferenceController(this.inferencer, this,
                            this.inferredContextPrefix, this.scheduler, this.currentClosureStatus);
                }
            }
        }
        return this.controller;
    }

    // Rewrite the query so to remove inferred contexts from the query dataset and
    // possibly from the query string. If the rewritten query has been judged trivial,
    // return the trivial result (i.e., false and empty iterations)

    @Override
    public void query(final QuerySpec<?> query, final Dataset dataset, final BindingSet bindings,
            final InferenceMode mode, final int timeout, final Object handler)
            throws QueryEvaluationException, RepositoryException
    {
        Preconditions.checkNotNull(handler);

        queryHelper(query, dataset, bindings, mode, timeout, handler);
    }

    @Override
    public <T> T query(final QuerySpec<T> query, final Dataset dataset, final BindingSet bindings,
            final InferenceMode mode, final int timeout) throws QueryEvaluationException,
            RepositoryException
    {
        return queryHelper(query, dataset, bindings, mode, timeout, null);
    }

    private <T> T queryHelper(final QuerySpec<T> query, final Dataset dataset,
            final BindingSet bindings, final InferenceMode mode, final int timeout,
            final Object handler) throws QueryEvaluationException, RepositoryException
    {
        Preconditions.checkNotNull(query); // fail-fast

        Dataset actualDataset = dataset;
        QuerySpec<T> actualQuery = query;

        if (actualDataset != null && (mode.isBackwardEnabled() || !mode.isForwardEnabled())) {
            actualQuery = actualQuery.enforceDataset(actualDataset);
            actualDataset = null;
            LOGGER.debug("[{}] Query modified to enforce supplied dataset", getID());
        }

        if (mode.isBackwardEnabled()) {
            final QuerySpec<T> old = actualQuery;
            actualQuery = getInferenceController(true).rewriteQuery(old,
                    this.currentClosureStatus, mode.isForwardEnabled());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] Rewritten query for backward inference is {}", getID(),
                        actualQuery == null ? "null" : actualQuery != old ? "different"
                                : "unchanged");
            }
        }

        if (!mode.isForwardEnabled() && actualQuery != null) {
            final QuerySpec<T> old = actualQuery;
            actualQuery = excludeInferredGraphs(actualQuery);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] Rewritten query to exclude closure is {}", getID(),
                        actualQuery == null ? "null" : actualQuery != old ? "different"
                                : "unchanged");
            }
        }

        if (actualQuery == null) {
            LOGGER.debug("[{}] Returning trivial result", getID());
            return trivialResult(query, handler);
        } else if (handler == null) {
            return delegate().query(actualQuery, actualDataset, bindings, mode, timeout);
        } else {
            delegate().query(actualQuery, actualDataset, bindings, mode, timeout, handler);
            return null;
        }
    }

    private <T> QuerySpec<T> excludeInferredGraphs(final QuerySpec<T> query)
    {
        final Entry<QueryModelNode, Dataset> entry = Algebra.excludeReadingGraphs(
                this.inferredContextPrefix, query.getExpression(), query.getDataset());

        if (entry == null) {
            return null;
        } else if (entry.getKey() == query.getExpression()
                && entry.getValue() == query.getDataset()) {
            return query;
        } else {
            return QuerySpec.from(query.getType(), (TupleExpr) entry.getKey(), entry.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T trivialResult(final QuerySpec<T> query, final Object handler)
            throws QueryEvaluationException
    {
        if (query.getType() == QueryType.BOOLEAN) {
            return (T) Boolean.FALSE;

        } else if (query.getType() == QueryType.TUPLE) {
            final TupleQueryResult emptyResult = new TupleQueryResultImpl(
                    ImmutableList.copyOf(query.getExpression().getBindingNames()),
                    Collections.<BindingSet>emptyList());
            if (handler == null) {
                return (T) emptyResult;
            } else {
                try {
                    QueryResultUtil.report(emptyResult, (TupleQueryResultHandler) handler);
                } catch (final TupleQueryResultHandlerException ex) {
                    throw new QueryEvaluationException(ex);
                }
            }

        } else if (query.getType() == QueryType.GRAPH) {
            final GraphQueryResult emptyResult = new GraphQueryResultImpl(query.getNamespaces(),
                    Collections.<Statement>emptyList());
            if (handler == null) {
                return (T) emptyResult;
            } else {
                try {
                    QueryResultUtil.report(emptyResult, (RDFHandler) handler);
                } catch (final RDFHandlerException ex) {
                    throw new QueryEvaluationException(ex);
                }
            }
        }

        return null;
    }

    @Override
    public CloseableIteration<? extends Resource, RepositoryException> getContextIDs(
            final InferenceMode mode) throws RepositoryException
    {
        if (mode.isBackwardEnabled()) {
            LOGGER.debug("[{}] Retrieving contexts using query", getID());
            final String query = "SELECT DISTINCT ?context "
                    + "WHERE { GRAPH ?context { ?subject ?predicate ?object } }";
            return Iterations.asRepositoryIteration(Iterations.project(
                    readWithQuery(QueryType.TUPLE, query, mode), "context", Resource.class));

        } else if (mode.isForwardEnabled()) {
            return delegate().getContextIDs(mode);

        } else {
            LOGGER.debug("[{}] Retrieving contexts filtering out inferred ones", getID());
            return Iterations.filter(delegate().getContextIDs(mode),
                    Predicates.not(this.inferredContextPrefix.valueMatcher()));
        }
    }

    /**
     * {@inheritDoc} One among four execution strategies is chosen, based on whether backward
     * reasoning and/or filtering of closure statements must be performed:
     * <ul>
     * <li><i>delegation</i>, in case no backward reasoning or filtering is required, or if just
     * filtering is required but we know exactly the target contexts of the operation;</li>
     * <li><i>delegation with result filtering</i>, in case filtering but not backward reasoning
     * is required, target contexts are unspecified and BNode matching is involved;</li>
     * <li><i>query</i> if either backward reasoning or filtering with unknown target contexts and
     * no BNode matching is involved;</li>
     * <li><i>empty results</i> if both backward reasoning and filtering are required, and no
     * target context remains after excluding closure contexts.</li>
     * </ul>
     */
    @Override
    public CloseableIteration<? extends Statement, RepositoryException> getStatements(
            @Nullable final Resource subj, @Nullable final URI pred, @Nullable final Value obj,
            final InferenceMode mode, final Resource... contexts) throws RepositoryException
    {
        final Resource[] actualContexts = mode.isForwardEnabled() ? contexts : Contexts.filter(
                contexts, this.inferredContextPrefix.valueMatcher(), null);

        if (!mode.isBackwardEnabled()) {
            if (actualContexts == Contexts.NONE) {
                LOGGER.debug("[{}] Reporting no statements after filtering inferred contexts",
                        getID());
                return new EmptyIteration<Statement, RepositoryException>();

            } else if (mode.isForwardEnabled() || actualContexts != Contexts.UNSPECIFIED) {
                return delegate().getStatements(subj, pred, obj, mode, actualContexts);

            } else if (subj instanceof BNode || obj instanceof BNode) {
                LOGGER.debug("[{}] Retrieving statements filtering out inferred ones", getID());
                return Iterations.filter(
                        delegate().getStatements(subj, pred, obj, mode, actualContexts),
                        Predicates.not(this.inferredContextPrefix.contextMatcher()));
            }
        }

        LOGGER.debug("[{}] Retrieving statements using query", getID());
        final String queryString = composeQuery(List.class, subj, pred, obj, actualContexts,
                mode.isBackwardEnabled());
        return Iterations.asRepositoryIteration(Iterations.asGraphQueryResult(
                readWithQuery(QueryType.TUPLE, queryString, mode),
                Collections.<String, String>emptyMap(), getValueFactory()));
    }

    /**
     * {@inheritDoc} Works similarly to
     * {@link #getStatements(Resource, URI, Value, InferenceMode, Resource...)}, with the only
     * exception that a boolean ASK query is issued instead of a SELECT tuple one.
     */
    @Override
    public boolean hasStatement(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final InferenceMode mode, final Resource... contexts)
            throws RepositoryException
    {
        final Resource[] actualContexts = mode.isForwardEnabled() ? contexts : Contexts.filter(
                contexts, this.inferredContextPrefix.valueMatcher(), null);

        if (!mode.isBackwardEnabled()) {
            if (actualContexts == Contexts.NONE) {
                LOGGER.debug("[{}] Reporting no statements exists after filtering "
                        + "inferred contexts", getID());
                return false;

            } else if (mode.isForwardEnabled() || actualContexts != Contexts.UNSPECIFIED) {
                return delegate().hasStatement(subj, pred, obj, mode, actualContexts);

            } else if (subj instanceof BNode || obj instanceof BNode) {
                return Iterations.getFirst(getStatements(subj, pred, obj, mode, actualContexts),
                        null) != null;
            }
        }

        LOGGER.debug("[{}] Checking for statement existence using query", getID());
        final String queryString = composeQuery(Boolean.class, subj, pred, obj, actualContexts,
                mode.isBackwardEnabled());
        return readWithQuery(QueryType.BOOLEAN, queryString, mode);
    }

    @Override
    public long size(final InferenceMode mode, final Resource... contexts)
            throws RepositoryException
    {
        final Resource[] actualContexts = mode.isForwardEnabled() ? contexts : Contexts.filter(
                contexts, this.inferredContextPrefix.valueMatcher(), null);

        if (!mode.isBackwardEnabled()) {
            if (actualContexts == Contexts.NONE) {
                LOGGER.debug("[{}] Reporting size 0 after filtering inferred contexts", getID());
                return 0L;

            } else if (mode.isForwardEnabled() || actualContexts != Contexts.UNSPECIFIED) {
                // return delegate().size(mode, actualContexts); // TODO: hack for OWLIM
            }
        }

        LOGGER.debug("[{}] Computing size using query", getID());
        final String queryString = composeQuery(Long.class, null, null, null, actualContexts,
                mode.isBackwardEnabled());
        final BindingSet bindings = Iterations.getOnlyElement(Iterations
                .asRepositoryIteration(readWithQuery(QueryType.TUPLE, queryString, mode)));
        return ((Literal) bindings.getValue("n")).longValue();
    }

    /**
     * Returns a SPARQL query string that matches statements having the supplied subject,
     * predicate, object and contexts (wildcard allowed) and returns one among three possible
     * results. Parameter <tt>resultType</tt> controls the result returned by the query, which can
     * be the {@link List} of matched statements, a {@link Long} consisting in the number of
     * matched statements or a {@link Boolean} that is true if at least a statement was matched.
     * Note that none of the components must be a BNode, as it is not possible to match statements
     * with BNodes with a SPARQL query.
     * 
     * @param resultType
     *            the expected result of the query to compose; acceptable values are the class
     *            objects for {@link List}, {@link Long} and {@link Boolean}.
     * @param subj
     *            the subject to match, not a BNode and null if wildcard
     * @param pred
     *            the predicate to match, null if wildcard
     * @param obj
     *            the object to match, not a BNode and null if wildcard
     * @param contexts
     *            the contexts to match, not BNodes; empty array matches any context
     * @param distinct
     *            whether the query must return distinct quadruples
     * @return the composed query string
     */
    private static String composeQuery(final Class<?> resultType, @Nullable final Resource subj,
            @Nullable final URI pred, @Nullable final Value obj, final Resource[] contexts,
            final boolean distinct)
    {
        boolean hasBNode = subj instanceof BNode || obj instanceof BNode;
        final StringBuilder fromBuilder = new StringBuilder();
        for (final Resource context : contexts) {
            hasBNode |= context instanceof BNode;
            fromBuilder.append("FROM NAMED ").append(SparqlRenderer.render(context).toString())
                    .append("\n");
        }
        final String from = fromBuilder.toString();

        // Throw an exception if subject, object or a context is a BNode, as we cannot match it
        // using a SPARQL query.
        if (hasBNode) {
            throw new IllegalArgumentException("Cannot compose a SPARQL query "
                    + "that matches statements whose subject, object or context is a blank node");
        }

        final String subjStr = subj == null ? "?subject" : SparqlRenderer.render(subj).toString();
        final String predStr = pred == null ? "?predicate" : SparqlRenderer.render(pred)
                .toString();
        final String objStr = obj == null ? "?object" : SparqlRenderer.render(obj).toString();
        final String where = String.format("WHERE { GRAPH ?context { %s %s %s } }\n", subjStr,
                predStr, objStr);

        if (resultType == List.class) {
            return String.format("SELECT %s ?context %s %s %s\n %s%s", //
                    distinct ? "DISTINCT" : "", //
                    subj == null ? "?subject" : "(" + subjStr + " AS ?subject)", //
                    pred == null ? "?predicate" : "(" + predStr + " AS ?predicate)", //
                    obj == null ? "?object" : "(" + objStr + " AS ?object)", //
                    from, where);
        } else if (resultType == Boolean.class) {
            return "ASK " + from + where;
        } else if (resultType == Long.class) {
            return "SELECT (COUNT(" + (distinct ? "DISTINCT " : "") + "*) AS ?n)\n" + from + where;
        } else {
            throw new Error("Unexpected result type: " + resultType);
        }
    }

    private <T> T readWithQuery(final QueryType<T> queryType, final String queryString,
            final InferenceMode mode) throws RepositoryException
    {
        try {
            return query(QuerySpec.from(queryType, queryString), null, null, mode, 0);

        } catch (final MalformedQueryException ex) {
            throw new Error("Unexpected exception", ex);
        } catch (final QueryEvaluationException ex) {
            throw new RepositoryException(ex);
        }
    }

    @Override
    public void update(final UpdateSpec update, final Dataset dataset, final BindingSet bindings,
            final InferenceMode mode) throws UpdateExecutionException, RepositoryException
    {
        Preconditions.checkNotNull(update); // fail-fast

        Dataset actualDataset = dataset;
        UpdateSpec actualUpdate = update;

        if (actualDataset != null && (mode.isBackwardEnabled() || !mode.isForwardEnabled())) {
            actualUpdate = actualUpdate.enforceDataset(actualDataset);
            actualDataset = null;
            LOGGER.debug("[{}] Update modified to enforce supplied dataset", getID());
        }

        if (mode.isBackwardEnabled()) {
            final UpdateSpec old = actualUpdate;
            actualUpdate = getInferenceController(true).rewriteUpdate(old,
                    this.currentClosureStatus, mode.isForwardEnabled());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] Rewritten update for backward inference is {}", getID(),
                        actualUpdate == null ? "null" : actualUpdate != old ? "different"
                                : "unchanged");
            }
        }

        if (!mode.isForwardEnabled() && actualUpdate != null) {
            final UpdateSpec u = actualUpdate;
            actualUpdate = excludeInferredGraphs(u);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] Rewritten update to exclude closure is {}", getID(),
                        actualUpdate == null ? "null" : actualUpdate != u ? "different"
                                : "unchanged");
            }
        }

        if (actualUpdate != null) {
            delegate().update(actualUpdate, actualDataset, bindings, mode);
        } else {
            LOGGER.info("[{}] Rewritten update not executed as trivial", getID());
        }
    }

    private UpdateSpec excludeInferredGraphs(final UpdateSpec update)
    {
        final int count = update.getExpressions().size();

        final List<UpdateExpr> exprs = Lists.newArrayListWithCapacity(count);
        final List<Dataset> datasets = Lists.newArrayListWithCapacity(count);
        boolean modified = false;

        for (int i = 0; i < count; ++i) {
            final UpdateExpr expr = update.getExpressions().get(i);
            final Dataset dataset = update.getDatasets().get(i);
            final Entry<QueryModelNode, Dataset> entry = Algebra.excludeReadingGraphs(
                    this.inferredContextPrefix, expr, dataset);
            if (entry == null) {
                modified = true;
            } else {
                exprs.add((UpdateExpr) entry.getKey());
                datasets.add(entry.getValue());
                modified |= entry.getKey() != expr || entry.getValue() != dataset;
            }
        }

        return modified ? UpdateSpec.from(exprs, datasets, update.getNamespaces()) : update;
    }

    // Notify the inference session. On failure, do not specify the added statements.

    @Override
    public void add(final Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException
    {
        // Check in advance to avoid notifying addition of unspecified statements.
        Preconditions.checkNotNull(statements);
        Preconditions.checkNotNull(contexts);

        // Must be acquired before issuing the operation.
        final InferenceController controller = getInferenceController(true);

        Iterable<? extends Statement> statementsToNotify = null;
        try {
            delegate().add(statements, contexts);
            statementsToNotify = statements;

        } finally {
            controller.statementsAdded(statementsToNotify, contexts);
            if (this.inferencer.getInferenceMode().isForwardEnabled()) {
                final ClosureStatus oldStatus = this.currentClosureStatus;
                this.currentClosureStatus = oldStatus.getStatusAfterStatementsAdded();
                if (this.currentClosureStatus != oldStatus) {
                    LOGGER.debug("[{}] Closure status after statements addition changed to {}",
                            getID(), this.currentClosureStatus);
                }
            }
        }
    }

    /**
     * {@inheritDoc} Delegates to the underlying transaction, notifies the inference session and
     * updates the closure status. Note that on failure, the inference session is notified of a
     * generic statements removal (null statements argument) as we don't know which and how many
     * statements were deleted (we expect this situation to be likely handled with a rollback).
     */
    @Override
    public void remove(final Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException
    {
        // Check in advance to avoid notification if input parameters are wrong.
        Preconditions.checkNotNull(statements);
        Preconditions.checkNotNull(contexts);

        // Must be acquired before issuing the operation.
        final InferenceController controller = getInferenceController(true);

        Iterable<? extends Statement> statementsToNotify = null;
        try {
            delegate().remove(statements, contexts);
            statementsToNotify = statements;

        } finally {
            controller.statementsRemoved(statementsToNotify, contexts);
            if (this.inferencer.getInferenceMode().isForwardEnabled()) {
                this.currentClosureStatus = this.currentClosureStatus
                        .getStatusAfterStatementsRemoved();
                LOGGER.debug("[{}] Closure status after statements removal is {}", getID(),
                        this.currentClosureStatus);
            }
        }
    }

    @Override
    public void remove(final Resource subj, final URI pred, final Value obj,
            final Resource... contexts) throws RepositoryException
    {
        if (subj != null && pred != null && obj != null) {
            final Statement statement = getValueFactory().createStatement(subj, pred, obj);
            remove(Collections.singleton(statement), contexts);

        } else if (subj == null && pred == null && obj == null
                && Contexts.UNSPECIFIED.equals(contexts)) {
            reset();

        } else {
            // Check in advance to avoid notification if input parameters are wrong.
            Preconditions.checkNotNull(contexts);

            // Must be acquired before issuing the operation.
            final InferenceController controller = getInferenceController(true);

            try {
                delegate().remove(subj, pred, obj, contexts);
            } finally {
                controller.statementsRemoved(null, contexts);
                if (this.inferencer.getInferenceMode().isForwardEnabled()) {
                    this.currentClosureStatus = this.currentClosureStatus
                            .getStatusAfterStatementsRemoved();
                    LOGGER.debug("[{}] Closure status after statements removal "
                            + "(with wildcards) is {}", getID(), this.currentClosureStatus);
                }
            }
        }
    }

    @Override
    public ClosureStatus getClosureStatus() throws RepositoryException
    {
        return this.currentClosureStatus;
    }

    @Override
    public void updateClosure() throws RepositoryException
    {
    	
        
        if (this.inferencer.getInferenceMode().isForwardEnabled() && this.currentClosureStatus != ClosureStatus.CURRENT) {
        	LOGGER.info("LOG: {}",currentClosureStatus);
            InferenceController controller= getInferenceController(true);
            LOGGER.info("{}",controller.toString());
            controller.updateClosure(this.currentClosureStatus);
            this.currentClosureStatus = ClosureStatus.CURRENT;
            LOGGER.info("[{}] Closure status after closure updated is {}", getID(),
                    this.currentClosureStatus);
        }
    }

    @Override
    public void clearClosure() throws RepositoryException
    {
        // Must be acquired before issuing the operation.
        final InferenceController controller = getInferenceController(true);

        if (!this.inferencer.getInferenceMode().isForwardEnabled()
                && this.currentClosureStatus == ClosureStatus.CURRENT) {
            return;
        }

        boolean success = false;
        try {
        	LOGGER.info("PREFIXXX: {}" , inferredContextPrefix);
            LOGGER.info("[{}] Clearing closure)", getID());
            final List<Resource> implicitContexts = Iterations.getAllElements(Iterations.filter(
                    getContextIDs(InferenceMode.FORWARD),
                    this.inferredContextPrefix.valueMatcher()));
            for (final Resource implicitContext : implicitContexts) {
            	LOGGER.info("[{}]",implicitContext);
                delegate().remove(null, null, null, new Resource[] { implicitContext });
            }
            success = true;

        } finally {
            if (success) {
                controller.statementsCleared(true);
                if (this.inferencer.getInferenceMode().isForwardEnabled()) {
                    this.currentClosureStatus = ClosureStatus.POSSIBLY_INCOMPLETE;
                } else {
                    this.currentClosureStatus = ClosureStatus.CURRENT;
                }
                LOGGER.info("[{}] Closure status after closure cleared is {}", getID(),
                        this.currentClosureStatus);

            } else {
                controller.statementsRemoved(null, Contexts.UNSPECIFIED);
                if (this.inferencer.getInferenceMode().isForwardEnabled()) {
                    this.currentClosureStatus = this.currentClosureStatus
                            .getStatusAfterStatementsRemoved();
                }
            }
        }
    }

    @Override
    public void reset() throws RepositoryException
    {
        // Must be acquired before issuing the operation.
        final InferenceController controller = getInferenceController(true);

        boolean success = false;
        try {
            delegate().reset();
            success = true;
        } finally {
            if (!success) {
                controller.statementsRemoved(null, Contexts.UNSPECIFIED);
                if (this.inferencer.getInferenceMode().isForwardEnabled()) {
                    this.currentClosureStatus = this.currentClosureStatus
                            .getStatusAfterStatementsRemoved();
                    LOGGER.debug("[{}] Reset failed, closure status set to {}", getID(),
                            this.currentClosureStatus);
                }
            }
        }

        controller.statementsCleared(false);

        if (this.inferencer.getInferenceMode().isForwardEnabled()) {
            this.currentClosureStatus = ClosureStatus.POSSIBLY_INCOMPLETE;
        } else {
            this.currentClosureStatus = ClosureStatus.CURRENT;
        }

        LOGGER.debug("[{}] Closure status after reset is {}", getID(), this.currentClosureStatus);

        updateClosure();
    }

    /**
     * {@inheritDoc} Notifies a previously allocated {@link InferencerSession} (if any) of the
     * closing transaction. This gives it the possibility to finalize a previously materialized
     * closure (e.g., removing auxiliary statements), or to write auxiliary data used in the
     * forward / backward inference process.
     */
    @Override
    public void end(final boolean commit) throws RepositoryException
    {
        final InferenceController controller = getInferenceController(false);
        if (controller != null) {
            controller.close(commit);
        }

        final boolean updateClosureMetadata = commit
                && this.originalClosureStatus != this.currentClosureStatus;

        // Note the status cache must be updated in two step, so that if we die after a successful
        // commit, the cache is empty and the right closure status will be fetched.

        boolean emptyRepository = false;
        if (updateClosureMetadata) {
            CLOSURE_STATUS_CACHE.invalidate(this.inferencer);
            emptyRepository = !delegate().hasStatement(null, null, null, InferenceMode.NONE);
            if (this.originalClosureStatus == ClosureStatus.CURRENT
                    || this.currentClosureStatus == ClosureStatus.STALE) {
                writeClosureMetadata(emptyRepository);
            }
        }

        delegate().end(commit);

        if (updateClosureMetadata) {
            if (this.originalClosureStatus == ClosureStatus.STALE
                    || this.currentClosureStatus == ClosureStatus.CURRENT) {
                writeClosureMetadata(emptyRepository);
            }
            CLOSURE_STATUS_CACHE.put(this.inferencer, this.currentClosureStatus);
        }
    }

}
