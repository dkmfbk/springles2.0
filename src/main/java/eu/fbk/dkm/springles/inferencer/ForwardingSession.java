package eu.fbk.dkm.springles.inferencer;

import javax.annotation.Nullable;

import com.google.common.collect.ForwardingObject;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.RepositoryException;

import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.base.QuerySpec;
import eu.fbk.dkm.springles.base.UpdateSpec;
import eu.fbk.dkm.springles.inferencer.Inferencer.Session;

public abstract class ForwardingSession extends ForwardingObject implements Session
{

    @Override
    protected abstract Session delegate();

    @Override
    public void statementsAdded(@Nullable final Iterable<? extends Statement> statements,
            final Resource... contexts) throws RepositoryException
    {
        this.delegate().statementsAdded(statements, contexts);
    }

    @Override
    public void statementsRemoved(@Nullable final Iterable<? extends Statement> statements,
            final Resource... contexts) throws RepositoryException
    {
        this.delegate().statementsRemoved(statements, contexts);
    }

    @Override
    public void statementsCleared(final boolean onlyClosure) throws RepositoryException
    {
        this.delegate().statementsCleared(onlyClosure);
    }

    @Override
    public <T> QuerySpec<T> rewriteQuery(final QuerySpec<T> query,
            final ClosureStatus closureStatus, final boolean forwardInferenceEnabled)
            throws RepositoryException
    {
        return this.delegate().rewriteQuery(query, closureStatus, forwardInferenceEnabled);
    }

    @Override
    public UpdateSpec rewriteUpdate(final UpdateSpec update, final ClosureStatus closureStatus,
            final boolean forwardInferenceEnabled) throws RepositoryException
    {
        return this.delegate().rewriteUpdate(update, closureStatus, forwardInferenceEnabled);
    }

    @Override
    public void updateClosure(final ClosureStatus closureStatus) throws RepositoryException
    {
        this.delegate().updateClosure(closureStatus);
    }

    @Override
    public void close(final boolean committing) throws RepositoryException
    {
        this.delegate().close(committing);
    }

}
