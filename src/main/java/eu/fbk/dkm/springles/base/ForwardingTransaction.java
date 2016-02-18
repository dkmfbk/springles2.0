package eu.fbk.dkm.springles.base;

import javax.annotation.Nullable;

import com.google.common.collect.ForwardingObject;

import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;

import info.aduna.iteration.CloseableIteration;

import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;

/**
 * Implementation of <tt>Transaction</tt> that forwards by default all method calls to a delegate
 * <tt>Transaction</tt>.
 * 
 * <p>
 * This class is modeled after the 'forwarding' pattern of Guava (see {@link ForwardingObject}).
 * Subclasses must implement method {@link #delegate()} which is called by other methods and must
 * provide the delegate {@link Transaction} instance.
 * </p>
 */
public abstract class ForwardingTransaction extends ForwardingObject implements Transaction
{

    /**
     * {@inheritDoc}
     */
    @Override
    protected abstract Transaction delegate();

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public String getID()
    {
        return delegate().getID();
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public ValueFactory getValueFactory()
    {
        return delegate().getValueFactory();
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public String getNamespace(final String prefix) throws RepositoryException
    {
        return delegate().getNamespace(prefix);
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public CloseableIteration<? extends Namespace, RepositoryException> getNamespaces()
            throws RepositoryException
    {
        return delegate().getNamespaces();
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public void setNamespace(final String prefix, @Nullable final String name)
            throws RepositoryException
    {
        delegate().setNamespace(prefix, name);
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public void clearNamespaces() throws RepositoryException
    {
        delegate().clearNamespaces();
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public void query(final QuerySpec<?> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode, final int timeout,
            final Object handler) throws QueryEvaluationException, RepositoryException
    {
        delegate().query(query, dataset, bindings, mode, timeout, handler);
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public <T> T query(final QuerySpec<T> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode, final int timeout)
            throws QueryEvaluationException, RepositoryException
    {
        return delegate().query(query, dataset, bindings, mode, timeout);
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public <T> T query(final URI queryURI, final QueryType<T> queryType, final InferenceMode mode,
            final Object... parameters) throws QueryEvaluationException, RepositoryException
    {
        return delegate().query(queryURI, queryType, mode, parameters);
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public CloseableIteration<? extends Resource, RepositoryException> getContextIDs(
            final InferenceMode mode) throws RepositoryException
    {
        return delegate().getContextIDs(mode);
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public CloseableIteration<? extends Statement, RepositoryException> getStatements(
            @Nullable final Resource subj, @Nullable final URI pred, @Nullable final Value obj,
            final InferenceMode mode, final Resource... contexts) throws RepositoryException
    {
        return delegate().getStatements(subj, pred, obj, mode, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public boolean hasStatement(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final InferenceMode mode, final Resource... contexts)
            throws RepositoryException
    {
        return delegate().hasStatement(subj, pred, obj, mode, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public long size(final InferenceMode mode, final Resource... contexts)
            throws RepositoryException
    {
        return delegate().size(mode, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public void update(final UpdateSpec update, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode)
            throws UpdateExecutionException, RepositoryException
    {
        delegate().update(update, dataset, bindings, mode);
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public void update(final URI updateURI, final InferenceMode mode, final Object... parameters)
            throws UpdateExecutionException, RepositoryException
    {
        delegate().update(updateURI, mode, parameters);
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public void add(final Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException
    {
        delegate().add(statements, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public void remove(final Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException
    {
        delegate().remove(statements, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public void remove(@Nullable final Resource subject, @Nullable final URI predicate,
            @Nullable final Value object, final Resource... contexts) throws RepositoryException
    {
        delegate().remove(subject, predicate, object, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public ClosureStatus getClosureStatus() throws RepositoryException
    {
        return delegate().getClosureStatus();
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public void updateClosure() throws RepositoryException
    {
        delegate().updateClosure();
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public void clearClosure() throws RepositoryException
    {
        delegate().clearClosure();
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public void reset() throws RepositoryException
    {
        delegate().reset();
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public <T, E extends Exception> T execute(final Operation<T, E> operation,
            final boolean writeOperation, final boolean closureNeeded) throws E,
            RepositoryException
    {
        return delegate().execute(operation, writeOperation, closureNeeded);
    }

    /**
     * {@inheritDoc} Delegates to wrapped transaction.
     */
    @Override
    public void end(final boolean commit) throws RepositoryException
    {
        delegate().end(commit);
    }

}
