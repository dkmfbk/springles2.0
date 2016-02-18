package eu.fbk.dkm.springles.base;

import javax.annotation.Nullable;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryReadOnlyException;

import eu.fbk.dkm.springles.InferenceMode;

final class UnmodifiableTransaction extends ForwardingTransaction
{

    private final Transaction delegate;

    public UnmodifiableTransaction(final Transaction delegate)
    {
        this.delegate = delegate;
    }

    @Override
    protected Transaction delegate()
    {
        return this.delegate;
    }

    @Override
    public void setNamespace(final String prefix, @Nullable final String name)
            throws RepositoryException
    {
        throw new RepositoryReadOnlyException();
    }

    @Override
    public void clearNamespaces() throws RepositoryException
    {
        throw new RepositoryReadOnlyException();
    }

    @Override
    public void update(final UpdateSpec update, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode)
            throws UpdateExecutionException, RepositoryException
    {
        throw new RepositoryReadOnlyException();
    }

    @Override
    public void update(final URI updateURI, final InferenceMode mode, final Object... parameters)
            throws UpdateExecutionException, RepositoryException
    {
        throw new RepositoryReadOnlyException();
    }

    @Override
    public void add(final Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException
    {
        throw new RepositoryReadOnlyException();
    }

    @Override
    public void remove(final Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException
    {
        throw new RepositoryReadOnlyException();
    }

    @Override
    public void remove(@Nullable final Resource subject, @Nullable final URI predicate,
            @Nullable final Value object, final Resource... contexts) throws RepositoryException
    {
        throw new RepositoryReadOnlyException();
    }

    @Override
    public void updateClosure() throws RepositoryException
    {
        throw new RepositoryReadOnlyException();
    }

    @Override
    public void clearClosure() throws RepositoryException
    {
        throw new RepositoryReadOnlyException();
    }

    @Override
    public void reset() throws RepositoryException
    {
        throw new RepositoryReadOnlyException();
    }

    @Override
    public <T, E extends Exception> T execute(final Operation<T, E> operation,
            final boolean writeOperation, final boolean closureNeeded) throws E,
            RepositoryException
    {
        if (!writeOperation) {
            return delegate().execute(operation, writeOperation, closureNeeded);
        } else {
            throw new RepositoryReadOnlyException();
        }
    }

}
