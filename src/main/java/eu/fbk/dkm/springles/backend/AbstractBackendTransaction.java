package eu.fbk.dkm.springles.backend;

import com.google.common.base.Preconditions;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.dkm.springles.base.QueryType;
import eu.fbk.dkm.springles.base.Transaction;

public abstract class AbstractBackendTransaction implements Transaction
{

    /** Shared log object. */
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBackendTransaction.class);

    private final String id;

    private final ValueFactory valueFactory;

    protected AbstractBackendTransaction(final String id, final ValueFactory valueFactory)
    {
        Preconditions.checkNotNull(id);
        Preconditions.checkNotNull(valueFactory);

        this.id = id;
        this.valueFactory = valueFactory;
    }

    @Override
    public final String getID()
    {
        return this.id;
    }

    @Override
    public final ValueFactory getValueFactory()
    {
        return this.valueFactory;
    }

    @Override
    public final <T> T query(final IRI queryURI, final QueryType<T> queryType,
            final InferenceMode mode, final Object... parameters)
    {
        throw new UnsupportedOperationException("No named query for URI: " + queryURI);
    }

    @Override
    public final void update(final IRI updateURI, final InferenceMode mode,
            final Object... parameters)
    {
        throw new UnsupportedOperationException("No named update for URI: " + updateURI);
    }

    /**
     * {@inheritDoc} Returns {@link ClosureStatus#CURRENT} as no inference is supported at this
     * level, and hence the closure is always empty and current.
     */
    @Override
    public final ClosureStatus getClosureStatus() throws RepositoryException
    {
        return ClosureStatus.CURRENT;
    }

    /**
     * {@inheritDoc} Does nothing, apart checking that the transaction status and writing support.
     */
    @Override
    public final void updateClosure() throws RepositoryException
    {
    }

    /**
     * {@inheritDoc} Does nothing, apart checking that the transaction status and writing support.
     */
    @Override
    public final void clearClosure() throws RepositoryException
    {
    }

    @Override
    public <T, E extends Exception> T execute(final Operation<T, E> operation,
            final boolean writeOperation, final boolean closureNeeded)
            throws E, RepositoryException
    {
        return operation.execute();
    }

    @Override
    public final void end(final boolean commit) throws RepositoryException
    {
        boolean success = false;
        try {
            this.doEnd(commit);
            success = true;

        } catch (final RuntimeException ex) {
            throw ex;
        } catch (final Exception ex) {
            throw new RepositoryException(ex.getMessage(), ex);

        } finally {
            if (commit && !success) {
                try {
                    this.doEnd(false);
                } catch (final Throwable ex) {
                    AbstractBackendTransaction.LOGGER
                            .error("Rollback failed after previous commit failure. Ignoring.", ex);
                }
            }

            try {
                this.doClose();
            } catch (final Throwable ex) {
                AbstractBackendTransaction.LOGGER.error(
                        "Exception caught while closing repository connection. Ignoring.", ex);
            }
        }
    }

    protected abstract void doEnd(boolean commit) throws Exception;

    protected abstract void doClose() throws Exception;

}
