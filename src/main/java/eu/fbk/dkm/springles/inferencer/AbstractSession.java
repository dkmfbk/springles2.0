package eu.fbk.dkm.springles.inferencer;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.RepositoryException;

import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.base.QuerySpec;
import eu.fbk.dkm.springles.base.UpdateSpec;
import eu.fbk.dkm.springles.inferencer.Inferencer.Session;

public abstract class AbstractSession implements Session
{

    @Override
    public void statementsAdded(@Nullable final Iterable<? extends Statement> statements,
            final Resource... contexts) throws RepositoryException
    {
    }

    @Override
    public void statementsRemoved(@Nullable final Iterable<? extends Statement> statements,
            final Resource... contexts) throws RepositoryException
    {
    }

    @Override
    public void statementsCleared(final boolean onlyClosure) throws RepositoryException
    {
    }

    @Override
    public <T> QuerySpec<T> rewriteQuery(final QuerySpec<T> query,
            final ClosureStatus closureStatus, final boolean forwardInferenceEnabled)
            throws RepositoryException
    {
        return query;
    }

    @Override
    public UpdateSpec rewriteUpdate(final UpdateSpec update, final ClosureStatus closureStatus,
            final boolean forwardInferenceEnabled) throws RepositoryException
    {
        return update;
    }

    @Override
    public void updateClosure(final ClosureStatus closureStatus) throws RepositoryException
    {
    }

    @Override
    public void close(final boolean committing) throws RepositoryException
    {
    }

}
