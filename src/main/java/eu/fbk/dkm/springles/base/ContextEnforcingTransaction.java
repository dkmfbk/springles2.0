package eu.fbk.dkm.springles.base;

import java.util.List;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.dkm.internal.util.Algebra;
import eu.fbk.dkm.internal.util.Contexts;
import eu.fbk.dkm.internal.util.Iterations;
import eu.fbk.dkm.internal.util.URIPrefix;
import eu.fbk.dkm.springles.InferenceMode;

/**
 * A <tt>Transaction</tt> decorator reformulating operations targeted at the null context and
 * preventing writing to inferred contexts.
 * <p>
 * This class checks the contexts supplied at the API level and performs two functions: (1) it
 * re-targets read and write operations addressing the null context to its named replacement and
 * (2) prevents writing (add, remove and update operations) to inferred contexts. Note that a
 * third context-related enforcing operation - namely, blocking access to inferred statements and
 * contexts when forward inference is not demanded - is not implemented here but is demanded to
 * the inference logic on the store side, as it is closely related to backward reasoning support.
 * </p>
 */
final class ContextEnforcingTransaction extends ForwardingTransaction
{

    // IMPLEMENTATION NOTE: queries are not filtered as there is no way in Sesame, for the time
    // being, to address the null context (there is, however, some discussion about introducing a
    // URI for it). It is only possible to address the default graph, which includes statements in
    // all contexts, thus including the null one but also its replacement (so no need to rewrite).

    /** Shared log object. */
    private static final Logger LOGGER = LoggerFactory
            .getLogger(ContextEnforcingTransaction.class);

    /** The underlying transaction this wrapper delegates to. */
    private final Transaction delegate;

    /** The IRI assigned to Sesame null context. */
    private final IRI nullContextURI;

    /** The IRI prefix satisfied by the IRIs of inferred Sesame contexts. */
    private final URIPrefix inferredContextPrefix;

    /**
     * Creates a new instance wrapping the transaction specified.
     *
     * @param delegate
     *            the transaction this wrapper delegates to
     * @param nullContextURI
     *            the URI assigned to Sesame null context
     * @param inferredContextPrefix
     *            the URI prefix satisfied by the URIs of inferred contexts
     */
    public ContextEnforcingTransaction(final Transaction delegate, final IRI nullContextURI,
            final URIPrefix inferredContextPrefix)
    {
        Preconditions.checkNotNull(delegate);
        Preconditions.checkNotNull(nullContextURI);
        Preconditions.checkNotNull(inferredContextPrefix);

        this.delegate = delegate;
        this.nullContextURI = nullContextURI;
        this.inferredContextPrefix = inferredContextPrefix;
    }

    /**
     * {@inheritDoc} Returns the underlying transaction set at construction time.
     */
    @Override
    protected Transaction delegate()
    {
        return this.delegate;
    }

    /**
     * Rewrites the contexts in the supplied array, replacing the null context with
     * {@link #nullContextURI}. The input array is not changed and is returned in case rewriting
     * is unnecessary.
     *
     * @param contexts
     *            the array of context identifiers to be rewritten
     * @return the rewritten contexts array
     * @see Contexts#rewrite(Resource[], Resource)
     */
    private Resource[] rewrite(final Resource[] contexts)
    {
        return Contexts.rewrite(contexts, this.nullContextURI);
    }

    /**
     * Rewrites and filters the supplied context array, replacing the null context with
     * {@link #nullContextURI} and removing inferred contexts. A warning message is logged in case
     * removal occurs. The input array is not changed.
     *
     * @param contexts
     *            the context array to rewrite and filter
     * @return the resulting context array, possibly the input array unchanged
     * @see Contexts#rewriteAndFilter(Resource[], Resource, com.google.common.base.Predicate,
     *      String)
     */
    private Resource[] rewriteAndFilter(final Resource[] contexts)
    {
        final String message = "Detected write request affecting inferred contexts. {} inferred "
                + "contexts removed from the operation target. Operation will be performed on "
                + "remaining contexts (if any).";
        return Contexts.rewriteAndFilter(contexts, this.nullContextURI,
                this.inferredContextPrefix.valueMatcher(), message);
    }

    /**
     * Rewrites and filters the supplied statements, moving statements in the null context to
     * context {@link #nullContextURI} and removing statements in inferred contexts. The method
     * wraps the supplied iterable, so to filter and rewrite statements at iteration time. A
     * warning message is logged in case statement removal occurs.
     *
     * @param iterable
     *            the statement <tt>Iterable</tt> to process
     * @return a statement <tt>Iterable<tt> that operates rewriting and filtering at iteration
     *         time
     * @see Contexts#rewriteAndFilter(Iterable, org.openrdf.model.ValueFactory, Resource,
     *      com.google.common.base.Predicate, String)
     */
    private Iterable<Statement> rewriteAndFilter(final Iterable<? extends Statement> iterable)
    {
        final String message = "Detected write request affecting inferred statements. {} inferred "
                + "statements removed from the operation target. Operation will be performed on "
                + "remaining statements (if any).";
        return Contexts.rewriteAndFilter(iterable, this.getValueFactory(), this.nullContextURI,
                this.inferredContextPrefix.valueMatcher(), message);
    }

    /**
     * {@inheritDoc} Delegates, replacing references to null context in the dataset with
     * references to its URI replacement.
     */
    @Override
    public void query(final QuerySpec<?> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode, final int timeout,
            final Object handler) throws QueryEvaluationException, RepositoryException
    {
        final Dataset actualDataset = dataset != null ? dataset : query.getDataset();
        final Entry<TupleExpr, Dataset> entry = Algebra
                .rewriteDefaultContext(query.getExpression(), actualDataset, this.nullContextURI);

        if (entry.getKey() == query.getExpression() && entry.getValue() == actualDataset) {
            this.delegate().query(query, dataset, bindings, mode, timeout, handler);
        } else {
            ContextEnforcingTransaction.LOGGER
                    .info("[{}] Query modified to replace null context in dataset "
                            + "with corresponding URI", this.getID());
            this.delegate().query(
                    QuerySpec.from(query.getType(), entry.getKey(), entry.getValue()), null,
                    bindings, mode, timeout, handler);
        }
    }

    /**
     * {@inheritDoc} Delegates, replacing references to null context in the dataset with
     * references to its URI replacement.
     */
    @Override
    public <T> T query(final QuerySpec<T> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode, final int timeout)
            throws QueryEvaluationException, RepositoryException
    {
        final Dataset actualDataset = dataset != null ? dataset : query.getDataset();
        final Entry<TupleExpr, Dataset> entry = Algebra
                .rewriteDefaultContext(query.getExpression(), actualDataset, this.nullContextURI);

        if (entry.getKey() == query.getExpression() && entry.getValue() == actualDataset) {
            return this.delegate().query(query, dataset, bindings, mode, timeout);
        } else {
            ContextEnforcingTransaction.LOGGER
                    .info("[{}] Query modified to replace null context in dataset "
                            + "with corresponding URI", this.getID());
            return this.delegate().query(
                    QuerySpec.from(query.getType(), entry.getKey(), entry.getValue()), null,
                    bindings, mode, timeout);
        }
    }

    /**
     * {@inheritDoc} Delegates, replacing the null context with the corresponding URI.
     */
    @Override
    public CloseableIteration<? extends Statement, RepositoryException> getStatements(
            @Nullable final Resource subj, @Nullable final IRI pred, @Nullable final Value obj,
            final InferenceMode mode, final Resource... contexts) throws RepositoryException
    {
        return this.delegate().getStatements(subj, pred, obj, mode, this.rewrite(contexts));
    }

    /**
     * {@inheritDoc} Delegates, replacing the null context with the corresponding URI.
     */
    @Override
    public boolean hasStatement(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final InferenceMode mode, final Resource... contexts)
            throws RepositoryException
    {
        return this.delegate().hasStatement(subj, pred, obj, mode, this.rewrite(contexts));
    }

    /**
     * {@inheritDoc} This method delegates, replacing the null context with the corresponding URI.
     */
    @Override
    public long size(final InferenceMode mode, final Resource... contexts)
            throws RepositoryException
    {
        return this.delegate().size(mode, this.rewrite(contexts));
    }

    /**
     * {@inheritDoc} Rewrites the update command so to avoid writing to inferred contexts, then
     * delegates.
     */
    @Override
    public void update(final UpdateSpec update, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode)
            throws UpdateExecutionException, RepositoryException
    {
        final int count = update.getExpressions().size();

        final List<UpdateExpr> exprs = Lists.newArrayListWithCapacity(count);
        final List<Dataset> datasets = Lists.newArrayListWithCapacity(count);
        boolean modified = false;

        for (int i = 0; i < count; ++i) {
            final UpdateExpr expr = update.getExpressions().get(i);
            final Dataset exprDataset = dataset != null ? dataset : update.getDatasets().get(i);

            Entry<UpdateExpr, Dataset> entry;
            entry = Algebra.rewriteDefaultContext(expr, exprDataset, this.nullContextURI);
            entry = Algebra.excludeWritingGraphs(this.inferredContextPrefix, entry.getKey(),
                    entry.getValue());

            if (entry == null) {
                modified = true;
            } else {
                exprs.add(entry.getKey());
                datasets.add(entry.getValue());
                final boolean exprModified = entry.getKey() != expr
                        || entry.getValue() != exprDataset;
                modified |= exprModified;
                if (exprModified && ContextEnforcingTransaction.LOGGER.isInfoEnabled()) {
                    ContextEnforcingTransaction.LOGGER
                            .info("[{}] Update modified to replace null context "
                                    + "with corresponding URI", this.getID());
                }
            }
        }

        if (!modified) {
            this.delegate().update(update, dataset, bindings, mode);
        } else if (!exprs.isEmpty()) {
            this.delegate().update(UpdateSpec.from(exprs, datasets, update.getNamespaces()), null,
                    bindings, mode);
        } else {
            ContextEnforcingTransaction.LOGGER.info(
                    "[{}] Update not executed as affects inferred contexts only", this.getID());
        }
    }

    /**
     * {@inheritDoc} Delegates after having modified the supplied parameters, in order to: (1)
     * avoid writing to inferred contexts and (2) add statements to the null context replacement
     * instead of the null context.
     */
    @Override
    public void add(final Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException
    {
        Preconditions.checkNotNull(statements); // fail-fast

        final Resource[] targetContexts = this.rewriteAndFilter(contexts);

        if (targetContexts == Contexts.NONE) {
            ContextEnforcingTransaction.LOGGER.info(
                    "[{}] add() operation not executed " + "as no explicit context is affected",
                    this.getID());
        } else if (targetContexts == Contexts.UNSPECIFIED) {
            this.delegate().add(this.rewriteAndFilter(statements), targetContexts);
        } else {
            this.delegate().add(statements, targetContexts);
        }
    }

    /**
     * {@inheritDoc} Delegates after having modified the supplied parameters, in order to: (1)
     * avoid removing statements from inferred contexts and (2) re-target the operation from the
     * null context to its replacement context.
     */
    @Override
    public void remove(final Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException
    {
        Preconditions.checkNotNull(statements); // fail-fast

        final Resource[] targetContexts = this.rewriteAndFilter(contexts);

        if (targetContexts == Contexts.NONE) {
            ContextEnforcingTransaction.LOGGER.info(
                    "[{}] remove() operation not executed " + "as no explicit context is affected",
                    this.getID());
        } else if (targetContexts == Contexts.UNSPECIFIED) {
            this.delegate().remove(this.rewriteAndFilter(statements), targetContexts);
        } else {
            this.delegate().remove(statements, targetContexts);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(@Nullable final Resource subject, @Nullable final IRI predicate,
            @Nullable final Value object, final Resource... contexts) throws RepositoryException
    {
        final Resource[] targetContexts = this.rewriteAndFilter(contexts);

        if (targetContexts == Contexts.NONE) {
            ContextEnforcingTransaction.LOGGER
                    .info("[{}] remove() operation (with wildcards) not executed "
                            + "as no explicit context is affected", this.getID());

        } else if (targetContexts == Contexts.UNSPECIFIED) {
            ContextEnforcingTransaction.LOGGER.info(
                    "[{}] retrieving explicit contexts to perform remove() operation",
                    this.getID());
            final List<Resource> explicitContexts = Iterations.getAllElements(
                    Iterations.filter(this.delegate().getContextIDs(InferenceMode.NONE),
                            Predicates.not(this.inferredContextPrefix.valueMatcher())));
            for (final Resource explicitContext : explicitContexts) {
                // XXX is iterating the best strategy here?
                this.delegate().remove(subject, predicate, object, explicitContext);
            }

        } else {
            this.delegate().remove(subject, predicate, object, contexts);
        }
    }

}
